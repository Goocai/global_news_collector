use axum::{extract::{State,Path}, Json, Router,  http::StatusCode,routing::{get, post}};
use serde::{Serialize, Deserialize};
use sqlx::PgPool;
use tracing::error;

#[derive(Serialize)]
pub struct PendingReviewItem {
    id: i32,
    news_title: String,
    probability_human: f64,   // 或 BigDecimal 转 f64
    probability_llm: f64,
    outcome_human: Option<i32>,
    outcome_llm: Option<i32>,
    target_date: chrono::NaiveDate,
    inference_human: String,
    inference_llm: String,
    position_size_pct_human: f64,
    position_size_pct_llm: f64
}

#[derive(Deserialize)]
pub struct PostMortemRequest {
    post_mortem: String,
}

pub fn routes() -> Router<PgPool> {
    Router::new()
        .route("/pending-reviews", get(list_pending_reviews))
        .route("/{id}/post_mortem", post(submit_post_mortem))
}

async fn list_pending_reviews(
    State(pool): State<PgPool>,
    // 暂时固定 user_id = 1，未来可从 AuthUser 提取
) -> Result<Json<Vec<PendingReviewItem>>, StatusCode> {
    // 查询同时连接 human_predictions 和 llm_predictions
    let rows = sqlx::query!(
        r#"
        SELECT
            p.id,
            n.title AS news_title,
            p.outcome_human,
            p.outcome_llm,
            p.target_date,
            hp.inference AS inference_human,
            hp.probability AS probability_human,
            hp.position_size_pct AS position_size_pct_human,
            lp.inference AS inference_llm,
            lp.probability AS probability_llm,
            lp.position_size_pct AS position_size_pct_llm
        FROM prediction_tasks p
        JOIN news n ON p.news_id = n.id
        JOIN human_predictions hp ON p.id = hp.task_id
        JOIN llm_predictions lp ON p.id = lp.task_id   -- 若存在无 LLM 的情况，可改为 LEFT JOIN
        WHERE p.user_id = $1
          AND p.outcome_human IS NOT NULL
          AND p.outcome_llm IS NOT NULL
          AND (p.post_mortem IS NULL OR p.post_mortem = '')
          AND (
              (hp.probability >= 70 AND p.outcome_human = 0)
              OR
              (hp.probability <= 30 AND p.outcome_human = 1)
          )
        ORDER BY p.submitted_at DESC
        "#,
        1  // 临时硬编码，后续可从认证中获取 user_id
    )
    .fetch_all(&pool)
    .await
    .map_err(|e| {
        error!("查询待审核列表失败: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // 将 BigDecimal 转为 f64，处理可能为 NULL 的仓位（LLM 允许 NULL）
    let items = rows
        .into_iter()
        .map(|row| {
            // 安全转换，如果解析失败则取默认值 0.0（实际业务中应保证数据有效）
            let prob_h = row.probability_human.to_string().parse::<f64>().unwrap_or(0.0);
            let prob_l = row.probability_llm.to_string().parse::<f64>().unwrap_or(0.0);
            let pos_h = row.position_size_pct_human.to_string().parse::<f64>().unwrap_or(0.0);
            let pos_l = row.position_size_pct_llm.to_string().parse::<f64>().unwrap_or(0.0);

            PendingReviewItem {
                id: row.id,
                news_title: row.news_title,
                probability_human: prob_h,
                probability_llm: prob_l,
                outcome_human: row.outcome_human,
                outcome_llm: row.outcome_llm,
                target_date: row.target_date,
                inference_human: row.inference_human,
                inference_llm: row.inference_llm,
                position_size_pct_human: pos_h,
                position_size_pct_llm: pos_l,
            }
        })
        .collect();

    Ok(Json(items))
}


async fn submit_post_mortem(
    State(pool): State<PgPool>,
    Path(id): Path<i32>,
    Json(payload): Json<PostMortemRequest>,
) -> StatusCode {
    if payload.post_mortem.trim().is_empty() {
        return StatusCode::BAD_REQUEST;
    }
    let result = sqlx::query!(
        "UPDATE prediction_tasks SET post_mortem = $1 WHERE id = $2 AND user_id = $3",
        payload.post_mortem,
        id,
        1
    )
    .execute(&pool)
    .await;
    match result {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}