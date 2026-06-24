use chrono::{DateTime, NaiveDate};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};
use tokio::time::{interval, Duration};
use tracing::{info, warn, error};
use serde::{Deserialize};
use bigdecimal::{BigDecimal, FromPrimitive};
// 暂未使用真实API，去除未使用的导入
// use reqwest::Client;
// use serde_json::json;

/// 启动 LLM 后台 Worker（每 x 秒扫描一次）
pub async fn start_llm_worker(pool: PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("启动 LLM 异步工作流 Worker");
    let mut interval = interval(Duration::from_secs(60));
    loop {
        interval.tick().await;
        if let Err(e) = process_one_batch(&pool).await {
            error!("LLM 工作批处理失败: {}", e);
        }
    }
}

/// 批量处理未生成 LLM 的人类预测（使用 FOR UPDATE SKIP LOCKED 避免并发冲突）
async fn process_one_batch(pool: &PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut tx = pool.begin().await?;

    // 查询需要处理的人类预测（llm_predicted=false 且没有对应的 LLM 记录）
    let rows = sqlx::query!(
        r#"
        SELECT p.id, p.news_id, p.extracted_facts,p.rule_json,p.target_date, p.user_id, n.title,n.description, n.content, n.url, n.published_at
        FROM prediction_tasks p
        JOIN news n ON p.news_id = n.id
        WHERE  p.llm_predicted = false
        ORDER BY p.id
        LIMIT 10
        FOR UPDATE SKIP LOCKED
        "#
    )
    .fetch_all(&mut *tx)
    .await?;

    if rows.is_empty() {
        tx.commit().await?;
        return Ok(());
    }

    info!("找到 {} 条待处理的人类预测", rows.len());

    for row in rows {
        // 如果 news_id 为空（异常数据），跳过该条并标记跳过
        if row.news_id.is_none() {
            sqlx::query!(
                "UPDATE prediction_tasks SET llm_predicted = true WHERE id = $1",
                row.id
            )
            .execute(&mut *tx)
            .await?;
            warn!("预测 {} 缺少 news_id，已跳过", row.id);
            continue;
        }

        // 熔断检查（传入 Option<i32>）
        let should_skip = check_meltdown(
            &mut tx,
            row.news_id.unwrap(),   // 已确保非 None
            &row.extracted_facts,
            row.published_at,      // 假设 published_at 为 NOT NULL，否则需要处理 Option
        )
        .await?;
        if should_skip {
            sqlx::query!(
                "UPDATE prediction_tasks SET llm_predicted = true WHERE id = $1",
                row.id
            )
            .execute(&mut *tx)
            .await?;
            warn!("预测 {} 触发熔断，已跳过 LLM 生成", row.id);
            continue;
        }

        // 调用 LLM 生成预测（模拟/真实）
        let llm_inference = call_llm(
            &row.title,
             row.content.as_deref(),
            &row.extracted_facts,
            &row.rule_json,
            &row.target_date
            ).await?;

        // 插入 LLM 预测记录（继承人类预测的 target_date 和 rule_json）
        let llm_record = sqlx::query!(
            r#"
            INSERT INTO llm_predictions (task_id, inference, inference_rule, probability, position_size_pct)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            "#,
            row.id,              // task_id
            &llm_inference.inference,       // inference (字符串)
            &llm_inference.rule_json,     // inference_rule (应该是 JSON 对象，如 serde_json::Value)
            &llm_inference.probability,         // probability (BigDecimal)
            &llm_inference.position_size_pct        // position_size_pct (BigDecimal)
        )
        .fetch_one(&mut *tx)      // 使用 execute，而非 fetch_one
        .await?;

        info!("为预测 {} 生成了 LLM 对照记录 {}", row.id, llm_record.id);
    }

    tx.commit().await?;
    Ok(())
}

/// 熔断检查
async fn check_meltdown(
    tx: &mut Transaction<'_, Postgres>,
    news_id: i32,
    extracted_facts: &String,
    published_at: DateTime<chrono::Utc>,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    // 1. 噪声标记检查（noise_flags 表中是否有记录）
    let noise_count = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM noise_flags WHERE news_id = $1",
        news_id
    )
    .fetch_one(&mut **tx)   // 使用 &mut **tx
    .await?
    .unwrap_or(0);         // COUNT(*) 永远返回非NULL，解包Option

    if noise_count > 0 {
        warn!("新闻 {} 被标记为噪声，触发熔断", news_id);
        return Ok(true);
    }

    // 2. 内容过短（< 100 字符）

    if extracted_facts.len() < 5 {
        warn!("新闻 {} 摘取内容过短 -> ({} 字符)，触发熔断", news_id, extracted_facts.len());
        return Ok(true);
    }

    // 3. 时效性：发布时间超过 3 天
    let now = chrono::Utc::now();
    if now.signed_duration_since(published_at).num_days() > 3 {
        warn!("新闻 {} 发布时间超过 3 天，触发熔断", news_id);
        return Ok(true);
    }

    Ok(false)
}

/// 调用 LLM API（当前使用模拟实现，返回一个占位推演）
/// 
#[derive(Deserialize)]
pub struct LlmResponse {
    inference: String,
    probability: BigDecimal,
    position_size_pct: BigDecimal,   // 修正字段名
    rule_json: serde_json::Value,
}

async fn call_llm(
    title: &str, 
    content: Option<&str>,
    extracted_facts: &str,
    rule_json:&Value,
    target_date:&NaiveDate

) -> Result<LlmResponse, Box<dyn std::error::Error + Send + Sync>> {
    // 模拟：生成一个简单推演文本
    // 后续可替换为真实的 Gemini/OpenAI 调用
    let _prompt = format!(
        "根据以下新闻标题和内容等信息，生成一个简要的概率推演（按照推演规则格式回复）。\n
        标题：{}，
        内容：{}，
        摘取的事实：{}，
        推演目标日期：{};
        推演规则：
        {}
        ",
        title,
        content.unwrap_or("（无正文）"),
        extracted_facts,
        target_date.to_string(),
        rule_json.to_string()
    );
    // 模拟延迟
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 返回一个固定格式的 LLM 推演（实际应调用 API）
    Ok(
        LlmResponse{
            inference:String::from("inference"),
            probability: BigDecimal::from_f64(0.5).unwrap(),
            position_size_pct:BigDecimal::from(0),
            rule_json: json!(rule_json),
        }
    )
    
    // 真实 API 调用示例（以 Gemini 为例，需要配置 API key）：
    // let client = Client::new();
    // let api_key = std::env::var("GEMINI_API_KEY")?;
    // let url = format!("https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={}", api_key);
    // let payload = json!({ "contents": [{ "parts": [{ "text": _prompt }] }] });
    // let resp = client.post(&url).json(&payload).send().await?;
    // let data: serde_json::Value = resp.json().await?;
    // let text = data["candidates"][0]["content"]["parts"][0]["text"].as_str().unwrap_or("").to_string();
    // Ok(text)
}