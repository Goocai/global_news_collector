use axum::{extract::{State,Path}, Json, Router,  http::StatusCode,routing::{get, post}};
use serde::{Serialize, Deserialize};
use sqlx::PgPool;

#[derive(Serialize)]
pub struct PendingReviewItem {
    id: i32,
    news_title: String,
    probability: f64,   // 或 BigDecimal 转 f64
    outcome: i32,
    target_date: chrono::NaiveDate,
    inference: String,
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
    // 暂时固定 user_id=1
) -> Json<Vec<PendingReviewItem>> {
    let rows = sqlx::query!(
        r#"
        SELECT p.id, n.title as news_title, p.probability, p.outcome, p.target_date, p.inference
        FROM predictions p
        JOIN news n ON p.news_id = n.id
        WHERE p.user_id = $1
          AND p.prediction_type = 'human'
          AND p.outcome IS NOT NULL
          AND (p.post_mortem IS NULL OR p.post_mortem = '')
          AND (
              (p.probability >= 70 AND p.outcome = 0)
              OR
              (p.probability <= 30 AND p.outcome = 1)
          )
        ORDER BY p.submitted_at DESC
        "#,
        1
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_else(|_| vec![]);

    let items = rows.into_iter().map(|row| {
        // 将 BigDecimal 转为 f64 或字符串，这里简化转为 f64
        let prob = row.probability.to_string().parse::<f64>().unwrap_or(0.0);
        PendingReviewItem {
            id: row.id,
            news_title: row.news_title,
            probability: prob,
            outcome: row.outcome.unwrap(),
            target_date: row.target_date,
            inference: row.inference,
        }
    }).collect();
    Json(items)
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
        "UPDATE predictions SET post_mortem = $1 WHERE id = $2 AND user_id = $3",
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