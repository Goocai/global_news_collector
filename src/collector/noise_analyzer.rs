use sqlx::PgPool;
use jieba_rs::Jieba;
use std::collections::HashMap;
use tracing::info;

//停用词常用列表
const STOP_WORDS: &[&str] = &["的", "了", "在", "是", "我", "有", "和", "就", "不", "人", "都", "一", "一个", "上", "也", "很", "到", "说", "要", "去", "你", "会", "着", "没有", "看", "好", "自己", "这",
    "那", "它", "他", "她", "我们", "你们", "他们", "它们", "什么", "怎么", "为什么", "如何", "因为", "所以", "但是", "而且", "如果", "那么",
];

/// 分析近 7 天噪声新闻的高频词，返回 Top N 个词
pub async fn extract_noise_keywords(
    pool: &PgPool,
    top_n: usize,
) ->Result<Vec<String>,sqlx::Error>{
    // 1. 查询近 7 天被标记为噪音的新闻标题
    let rows = sqlx::query!(
        r#"
        SELECT DISTINCT n.title
        FROM noise_flags nf
        JOIN news n ON nf.news_id = n.id
        WHERE nf.flagged_at > NOW() - INTERVAL '7 days' 
        "#,
    )
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(vec![]);
    }
    
    // 2. 分词并统计词频
    let jieba = Jieba::new();
    let mut word_count : HashMap<String,usize> = HashMap::new();
    for row in rows{
        let title  = row.title;
        let words = jieba.cut(&title, false); // false 表示不进行 HMM 分词（速度更快）
        for token in words{
            let word = token.word;
            // 过滤停用词、单字词、纯数字等
            if word.len()<=1 {
                continue;
            }

            if STOP_WORDS.contains(&word){
                continue;
            }

            if word.chars().all(|c| c.is_ascii_digit()){
                continue;
            }

            *word_count.entry(word.to_string()).or_insert(0)+=1;
        }

    }   

    // 3. 按词频排序，取 Top N
    let mut sorted: Vec<(String, usize)> = word_count.into_iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));

    let top_words: Vec<String> = sorted
        .into_iter()
        .take(top_n)
        .map(|(word, _)| word)
        .collect();

    if !top_words.is_empty() {
        info!("提取到噪声高频词: {:?}", top_words);
    }
    Ok(top_words)
        
}