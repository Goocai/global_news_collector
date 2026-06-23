use std::error::Error;
use sqlx::PgPool;
use reqwest::Client;
use rss::Channel;
use chrono::{DateTime, Utc, FixedOffset};
use tracing::{info,warn};
use scraper::{Html,Selector};
use legible::{parse,Options};
use std::time::Duration;
use crate::collector::DynamicExcludeWords;


/// 灵活解析 RSS 发布时间字符串
/// 支持标准 RFC2822、GMT 替换、RFC3339 等多种常见格式
fn parse_rss_pub_date(date_str: &str) -> Option<DateTime<FixedOffset>> {
    // 1. 尝试标准 RFC2822
    if let Ok(dt) = DateTime::parse_from_rfc2822(date_str) {
        return Some(dt);
    }
    // 2. 将 "GMT" 替换为 "+0000" 再试
    let normalized = date_str.replace("GMT", "+0000");
    if let Ok(dt) = DateTime::parse_from_rfc2822(&normalized) {
        return Some(dt);
    }
    // 3. 尝试 RFC3339 格式
    if let Ok(dt) = DateTime::parse_from_rfc3339(date_str) {
        return Some(dt);
    }
    // 都不成功
    None
}


/// 从RSS源获取新闻数据并存储到数据库中
/// 自动基于url去重
pub async fn fetch_and_store(
    pool:&PgPool,
    source_id: i32,
    url: &str,
    exclude_words: &DynamicExcludeWords,
) -> Result<(), Box<dyn Error+Send+Sync>>{
    let client = Client::new();
    //发送http 请求获取RSS内容
    let response = client.get(url).send().await?;
    let bytes = response.bytes().await?;
    //解析RSS内容 （rss crate 会自动处理 UTF-8 等）
    let  channel = Channel::read_from(&bytes[..])?;

    let mut inserted_count = 0;
    for item in channel.items() {
        // 1. 获取标题，无标题则跳过
        let title = match item.title() {
            Some(t) => t.to_string(),
            None => {
                warn!("跳过无标题的新闻，来源ID: {}", source_id);
                continue;
            }
        };

        // 2. 动态排除词检查
        {
            let exclude_guard = exclude_words.read().await;
            let should_exclude = exclude_guard.iter().any(|word| title.contains(word));
            drop(exclude_guard);
            if should_exclude {
                info!("新闻标题包含排除词，跳过: {}", title);
                continue;
            }
        }

        // 3. 获取链接
        let link = match item.link() {
            Some(l) => l.to_string(),
            None => {
                warn!("跳过无链接的新闻，标题: {}, 来源ID: {}", title, source_id);
                continue;
            }
        };

        // 4. 发布时间处理（同上）
        let published_at = match item.pub_date() {
            Some(date_str) => parse_rss_pub_date(date_str)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|| {
                    warn!("无法解析发布时间 '{}', 使用当前UTC时间，标题: {}", date_str, title);
                    Utc::now()
                }),
            None => {
                warn!("新闻缺少发布时间，使用当前UTC时间，标题: {}", title);
                Utc::now()
            }
        };

        // 5. 去重
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM news WHERE url = $1)"
        )
        .bind(&link)
        .fetch_one(pool)
        .await?;

        if exists {
            info!("新闻已存在，跳过插入，标题: {}", title);
            continue;
        }

        // 6. 摘要
        let description = item.description()
            .map(|d| d.to_string())
            .unwrap_or_else(|| {
                warn!("新闻缺少摘要，使用空字符串代替，标题: {}", title);
                String::new()
            });

        // 7. 插入数据库
        sqlx::query!(
            r#"
            INSERT INTO news (source_id, title, url, description, content, published_at)
            VALUES ($1, $2, $3, $4, NULL, $5)
            "#,
            source_id,
            title,        // 此时 title 是 String，类型正确
            link,
            description,
            published_at
        )
        .execute(pool)
        .await?;

        inserted_count += 1;
        info!("成功插入新闻，标题: {}", title);
    }
        
    info!("来源 {} 采集完成，新增 {} 条新闻", source_id, inserted_count);
    Ok(())
}



/// 从新闻链接抓取 HTML 并提取正文（净化后）
pub async fn fetch_full_content(client: &Client, url: &str) -> Option<String> {
    let resp = client.get(url).timeout(Duration::from_secs(10)).send().await.ok()?;
    let html = resp.text().await.ok()?;

    // 1. 尝试使用 legible 提取
    let options = Options::new()
        .char_threshold(200)    // 适应中文短文章
        .keep_classes(false);   // 不需要 CSS 类

    if let Ok(article) = parse(&html, Some(url), Some(options)) {
        let content_html = article.content;
        if content_html.len() > 100 {
            // 净化 HTML，避免 XSS
            let clean_html = ammonia::clean(&content_html);
            return Some(clean_html);
        }
    }

    // 2. 回退方案：提取所有 <p> 标签
    let document = Html::parse_document(&html);
    let p_selector = match Selector::parse("p") {
        Ok(s) => s,
        Err(_) => return None,
    };
    let paragraphs: Vec<String> = document
        .select(&p_selector)
        .map(|el| el.text().collect::<String>())
        .filter(|text| text.len() > 20)
        .collect();

    if paragraphs.is_empty() {
        None
    } else {
        let raw_html = paragraphs
            .iter()
            .map(|p| format!("<p>{}</p>", p))
            .collect::<Vec<_>>()
            .join("\n");
        // 对回退内容也进行净化
        Some(ammonia::clean(&raw_html))
    }
}