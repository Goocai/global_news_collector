mod rules;
use rules::{Rule,evaluate_rule};
use sqlx::PgPool;
use tokio::time::{interval, Duration};
use tracing::{info, error};


/// 主动判定未到期的预测（规则提前满足则证实）
async fn judge_active_predictions(pool: &PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut tx = pool.begin().await?;
    let rows = sqlx::query!(
        r#"
        SELECT id
        FROM prediction_tasks
        WHERE judge_status = 'pending'
          AND outcome_human IS NULL
          AND outcome_llm IS NULL
          AND target_date >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date
        FOR UPDATE SKIP LOCKED
        "#
    )
    .fetch_all(&mut *tx)
    .await?;

    for row in rows {
        let id = row.id;   // 直接使用，因为 id 是 i32，非 Option

        // ---- 处理 human_predictions ----
        let hprow = sqlx::query!(
            r#"
            SELECT inference_rule
            FROM human_predictions
            WHERE task_id = $1
            "#,
            id
        )
        .fetch_one(&mut *tx)
        .await?;

        match serde_json::from_value::<Rule>(hprow.inference_rule) {
            Ok(rule) => {
                match evaluate_rule(&rule, pool).await {
                    Ok(true) => {
                        sqlx::query!(
                            "UPDATE prediction_tasks 
                             SET outcome_human = 1, judge_status = 'resolved', verified_at = NOW() 
                             WHERE id = $1",
                            id
                        )
                        .execute(&mut *tx)
                        .await?;
                        info!("预测 {} 人类规则满足，自动证实", id);
                    }
                    Ok(false) => { /* 未满足，不做操作 */ }
                    Err(e) => {
                        error!("验证人类规则失败 预测 {}: {}", id, e);
                        sqlx::query!(
                            "UPDATE prediction_tasks SET judge_status = 'failed_api' WHERE id = $1",
                            id
                        )
                        .execute(&mut *tx)
                        .await?;
                    }
                }
            }
            Err(e) => {
                error!("解析人类规则 JSON 失败 预测 {}: {}", id, e);
                sqlx::query!(
                    "UPDATE prediction_tasks SET judge_status = 'failed_api' WHERE id = $1",
                    id
                )
                .execute(&mut *tx)
                .await?;
            }
        }

        // ---- 处理 llm_predictions ----
        let lprow = sqlx::query!(
            r#"
            SELECT inference_rule
            FROM llm_predictions
            WHERE task_id = $1
            "#,
            id
        )
        .fetch_one(&mut *tx)
        .await?;

        match serde_json::from_value::<Rule>(lprow.inference_rule) {
            Ok(rule) => {
                match evaluate_rule(&rule, pool).await {
                    Ok(true) => {
                        sqlx::query!(
                            "UPDATE prediction_tasks 
                             SET outcome_llm = 1, judge_status = 'resolved', verified_at = NOW() 
                             WHERE id = $1",
                            id
                        )
                        .execute(&mut *tx)
                        .await?;
                        info!("预测 {} LLM 规则满足，自动证实", id);
                    }
                    Ok(false) => { /* 未满足，不做操作 */ }
                    Err(e) => {
                        error!("验证 LLM 规则失败 预测 {}: {}", id, e);
                        sqlx::query!(
                            "UPDATE prediction_tasks SET judge_status = 'failed_api' WHERE id = $1",
                            id
                        )
                        .execute(&mut *tx)
                        .await?;
                    }
                }
            }
            Err(e) => {
                error!("解析 LLM 规则 JSON 失败 预测 {}: {}", id, e);
                sqlx::query!(
                    "UPDATE prediction_tasks SET judge_status = 'failed_api' WHERE id = $1",
                    id
                )
                .execute(&mut *tx)
                .await?;
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
        UPDATE prediction_tasks
        SET outcome_human = 0, outcome_llm = 0,judge_status = 'resolved', verified_at = NOW()
        WHERE judge_status = 'pending'
          AND outcome_human IS NULL
          AND outcome_llm IS NULL 
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
        UPDATE prediction_tasks
        SET outcome_human = 0, outcome_llm = 0, judge_status = 'resolved', verified_at = NOW()
        WHERE judge_status = 'failed_api'
          AND outcome_human IS NULL
          AND outcome_llm IS NULL 
          AND target_date < ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - INTERVAL '2 days')::date
        RETURNING id
        "#
    )
    .fetch_all(&mut *tx)
    .await?;

    if !updated_failed.is_empty() {
        info!("自动判负 {} 条预测（failed_api 超出缓冲期）", updated_failed.len());
    }

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