mod rules;
use rules::{
    RuleVerifier, PriceChangeVerifier, CentralBankVerifier,
    EconomicDataVerifier, UrlKeywordVerifier,
};

use serde_json::Value;
use sqlx::PgPool;
use tokio::time::{interval, Duration};
use tracing::{info, error};

// 辅助函数：根据 rule_json 调用对应判定器
async fn verify_rule(rule_json: &Value, pool: &PgPool) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let rule_type = rule_json["type"].as_str().unwrap_or("");
    match rule_type {
        "price_change" => {
            let verifier = PriceChangeVerifier;
            verifier.verify(rule_json, pool).await
        }
        "central_bank" => {
            let verifier = CentralBankVerifier;
            verifier.verify(rule_json, pool).await
        }
        "economic_data" => {
            let verifier = EconomicDataVerifier;
            verifier.verify(rule_json, pool).await
        }
        "url_keyword" => {
            let verifier = UrlKeywordVerifier;
            verifier.verify(rule_json, pool).await
        }
        _ => Ok(false),
    }
}

/// 主动判定未到期的预测（规则提前满足则证实）
async fn judge_active_predictions(pool: &PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut tx = pool.begin().await?;
    let rows = sqlx::query!(
        r#"
        SELECT id, rule_json
        FROM predictions
        WHERE judge_status = 'pending'
          AND outcome IS NULL
          AND target_date >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date
        FOR UPDATE SKIP LOCKED
        "#
    )
    .fetch_all(&mut *tx)
    .await?;

    for row in rows {
        if let Some(rule_json) = row.rule_json {
            match verify_rule(&rule_json, pool).await {
                Ok(true) => {
                    // 人类预测证实
                    sqlx::query!(
                        "UPDATE predictions SET outcome = 1, judge_status = 'resolved', verified_at = NOW() WHERE id = $1",
                        row.id
                    ).execute(&mut *tx).await?;
                    // 同步 LLM 预测
                    sqlx::query!(
                        "UPDATE predictions SET outcome = 1, judge_status = 'resolved', verified_at = NOW() WHERE parent_prediction_id = $1",
                        row.id
                    ).execute(&mut *tx).await?;
                    info!("预测 {} 规则满足，自动证实", row.id);
                }
                Ok(false) => {}
                Err(e) => {
                    error!("验证规则失败 预测 {}: {}", row.id, e);
                    // 可选：将状态置为 failed_api
                    sqlx::query!(
                        "UPDATE predictions SET judge_status = 'failed_api' WHERE id = $1",
                        row.id
                    ).execute(&mut *tx).await?;
                }
            }
        }
    }
    tx.commit().await?;
    Ok(())
}

/// 到期自动判负（pending 和 failed_api 缓冲）
pub async fn expire_overdue_predictions(pool: &PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut tx = pool.begin().await?;

    // pending 到期判负
    let updated_human = sqlx::query!(
        r#"
        UPDATE predictions
        SET outcome = 0, judge_status = 'resolved', verified_at = NOW()
        WHERE prediction_type = 'human'
          AND judge_status = 'pending'
          AND outcome IS NULL
          AND target_date < (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date
        RETURNING id
        "#
    )
    .fetch_all(&mut *tx)
    .await?;

    if !updated_human.is_empty() {
        info!("自动判负 {} 条人类预测（pending 到期）", updated_human.len());
    }

    // failed_api 缓冲期（超过2天）判负
    let updated_failed = sqlx::query!(
        r#"
        UPDATE predictions
        SET outcome = 0, judge_status = 'resolved', verified_at = NOW()
        WHERE prediction_type = 'human'
          AND judge_status = 'failed_api'
          AND outcome IS NULL
          AND target_date < ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - INTERVAL '2 days')::date
        RETURNING id
        "#
    )
    .fetch_all(&mut *tx)
    .await?;

    if !updated_failed.is_empty() {
        info!("自动判负 {} 条人类预测（failed_api 超出缓冲期）", updated_failed.len());
    }

    // 同步 LLM 预测
    let updated_llm = sqlx::query!(
        r#"
        UPDATE predictions llm
        SET outcome = 0, judge_status = 'resolved', verified_at = NOW()
        FROM predictions human
        WHERE llm.parent_prediction_id = human.id
          AND llm.prediction_type = 'llm'
          AND llm.outcome IS NULL
          AND human.judge_status = 'resolved'
          AND human.outcome = 0
        RETURNING llm.id
        "#
    )
    .fetch_all(&mut *tx)
    .await?;

    if !updated_llm.is_empty() {
        info!("同步判负 {} 条 LLM 预测", updated_llm.len());
    }

    tx.commit().await?;
    Ok(())
}

/// 启动判定 Worker（每小时执行一次）
pub async fn start_judge_worker(pool: PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("启动判定状态机 Worker");
    let mut interval = interval(Duration::from_secs(3600));
    loop {
        interval.tick().await;
        if let Err(e) = judge_active_predictions(&pool).await {
            error!("主动判定失败: {}", e);
        }
        if let Err(e) = expire_overdue_predictions(&pool).await {
            error!("到期判负失败: {}", e);
        }
    }
}