use axum::{
    Router, extract::State, http::StatusCode,
    response::Json, routing::post,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use chrono::NaiveDate;
use tracing::error;
use bigdecimal::BigDecimal;
use crate::auth::extractor::AuthUser;


#[derive(Deserialize)]
pub struct CreatePredictionRequest {
    news_id: i32,
    extracted_facts: String,
    inference: String,
    probability: BigDecimal,
    position_size_pct: BigDecimal,   // 修正字段名
    target_date: NaiveDate,
    rule_json: serde_json::Value,
}

#[derive(Serialize)]
pub struct PredictionResponse {
    id: i32,
    message: &'static str,
}

pub fn routes() -> Router<PgPool> {
    Router::new().route("/", post(create_prediction))
}


async fn create_prediction(
    State(pool): State<PgPool>,
    auth_user: AuthUser,
    Json(payload): Json<CreatePredictionRequest>,
) -> Result<Json<PredictionResponse>, StatusCode> {
    // 校验概率和仓位百分比
    let prob = payload.probability.to_string().parse::<f64>().map_err(|_| StatusCode::BAD_REQUEST)?;
    if !(0.0..=100.0).contains(&prob) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let pos = payload.position_size_pct.to_string().parse::<f64>().map_err(|_| StatusCode::BAD_REQUEST)?;
    if !(0.0..=100.0).contains(&pos) {
        return Err(StatusCode::BAD_REQUEST);
    }

    // 校验目标日期必须 > 今天
    let today_utc = chrono::Utc::now().date_naive();
    if payload.target_date <= today_utc {
        return Err(StatusCode::BAD_REQUEST);
    }

    let user_id = auth_user.user_id;

    // 插入预测任务主表，返回任务ID
    let task_id = sqlx::query!(
        r#"
        INSERT INTO prediction_tasks (
            news_id, user_id,
            extracted_facts, target_date, rule_json,
            judge_status
        ) VALUES ($1, $2, $3, $4, $5, 'pending')
        RETURNING id
        "#,
        payload.news_id,
        user_id,
        payload.extracted_facts,
        payload.target_date,
        payload.rule_json,
    )
    .fetch_one(&pool)
    .await
    .map_err(|e| {
        error!("插入预测任务失败: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?
    .id;   // 提取 id 字段

    // 插入人类预测详情
    sqlx::query!(
        r#"
        INSERT INTO human_predictions (
            task_id, inference, probability, position_size_pct
        ) VALUES ($1, $2, $3, $4)
        "#,
        task_id,
        payload.inference,
        payload.probability,
        payload.position_size_pct
    )
    .execute(&pool)   // 使用 execute 而非 fetch_one，因为不需要返回数据
    .await
    .map_err(|e| {
        error!("插入人类预测失败: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(PredictionResponse {
        id: task_id,
        message: "预测提交成功！",
    }))
}