use axum::{extract::{State,Path}, http::StatusCode, Json, Router, 
    routing::{get, post}};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use chrono::NaiveDate;
use tracing::error;
use crate::auth::extractor::AuthUser;


pub fn routes() -> Router<PgPool> {
    Router::new()
        .route("/failed-predictions", get(list_failed_predictions))
        .route("/predictions/{id}/override", post(override_prediction))
        .route("/sources", get(list_sources).post(create_source))
        .route("/sources/{id}", get(get_source).put(update_source).delete(delete_source))
        .route("/exclude-words", get(get_exclude_words))  // 查看当前动态排除词
}


#[derive(Deserialize)]
pub struct OverrideRequest {
    outcome: i32, // 0 或 1
}

#[derive(Serialize)]
pub struct FailedPredictionItem {
    id: i32,
    news_title: String,
    inference: String,
    probability: String, // BigDecimal 转字符串
    target_date: NaiveDate,
}


// 获取所有 failed_api 状态且未判决的预测
async fn list_failed_predictions(
    State(pool): State<PgPool>,
) -> Json<Vec<FailedPredictionItem>> {
    let rows = sqlx::query!(
        r#"
        SELECT p.id, n.title as news_title, p.inference, p.probability, p.target_date
        FROM predictions p
        JOIN news n ON p.news_id = n.id
        WHERE p.judge_status = 'failed_api' AND p.outcome IS NULL
        ORDER BY p.submitted_at DESC
        "#
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_else(|_| vec![]);

    let items = rows
        .into_iter()
        .map(|row| FailedPredictionItem {
            id: row.id,
            news_title: row.news_title,
            inference: row.inference,
            probability: row.probability.to_string(), // BigDecimal -> String
            target_date: row.target_date,
        })
        .collect();
    Json(items)
}

// 覆盖预测结果（管理员专用）
async fn override_prediction(
    State(pool): State<PgPool>,
    auth_user:AuthUser,
    axum::extract::Path(id): axum::extract::Path<i32>,
    Json(payload): Json<OverrideRequest>,
) -> StatusCode {

    if auth_user.role != "admin" {
        return StatusCode::FORBIDDEN;
    }
    
    if payload.outcome != 0 && payload.outcome != 1 {
        return StatusCode::BAD_REQUEST;
    }
    let result = sqlx::query!(
        r#"
        UPDATE predictions
        SET outcome = $1, judge_status = 'resolved', verified_at = NOW()
        WHERE id = $2 AND judge_status = 'failed_api' AND outcome IS NULL
        "#,
        payload.outcome,
        id
    )
    .execute(&pool)
    .await;
    match result {
        Ok(rows) if rows.rows_affected() == 1 => StatusCode::OK,
        Ok(_) => StatusCode::NOT_FOUND,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}


// ---------- Sources 管理 请求/响应结构 ----------
#[derive(Deserialize)]
pub struct CreateSourceRequest {
    pub name: String,
    pub url: String,
    pub feed_type: String, // rss, atom, api
    pub refresh_interval_sec: Option<i32>,
    pub enabled: Option<bool>,
    pub require_keywords: Option<Vec<String>>,
    pub exclude_keywords: Option<Vec<String>>,
}

// #[derive(Deserialize)]
// pub struct UpdateSourceRequest {
//     pub name: Option<String>,
//     pub url: Option<String>,
//     pub feed_type: Option<String>,
//     pub refresh_interval_sec: Option<i32>,
//     pub enabled: Option<bool>,
//     pub require_keywords: Option<Vec<String>>,
//     pub exclude_keywords: Option<Vec<String>>,
// }

#[derive(Serialize)]
pub struct SourceItem {
    pub id: i32,
    pub name: String,
    pub url: String,
    pub feed_type: String,
    pub refresh_interval_sec: i32,
    pub enabled: bool,
    pub require_keywords: Vec<String>,
    pub exclude_keywords: Vec<String>,
}



// ---------- 处理器 ----------
async fn list_sources(
    State(pool): State<PgPool>,
) -> Result<Json<Vec<SourceItem>>, StatusCode> {
    let rows = sqlx::query!(
        "SELECT id, name, url, feed_type, refresh_interval_sec, enabled, require_keywords, exclude_keywords FROM sources ORDER BY id"
    )
    .fetch_all(&pool)
    .await
    .map_err(|e| {
        error!("查询源列表失败: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let items = rows
        .into_iter()
        .map(|r| SourceItem {
            id: r.id,
            name: r.name,
            url: r.url,
            feed_type: r.feed_type,
            refresh_interval_sec: r.refresh_interval_sec.unwrap_or(300),
            enabled: r.enabled.unwrap_or(true),
            require_keywords: r.require_keywords.unwrap_or_default(),
            exclude_keywords: r.exclude_keywords.unwrap_or_default(),
        })
        .collect();
    Ok(Json(items))
}

async fn get_source(
    State(pool): State<PgPool>,
    Path(id): Path<i32>,
) -> Result<Json<SourceItem>, StatusCode> {
    let row = sqlx::query!(
        "SELECT id, name, url, feed_type, refresh_interval_sec, enabled, require_keywords, exclude_keywords FROM sources WHERE id = $1",
        id
    )
    .fetch_optional(&pool)
    .await
    .map_err(|e| {
        error!("查询源失败: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?
    .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(SourceItem {
        id: row.id,
        name: row.name,
        url: row.url,
        feed_type: row.feed_type,
        refresh_interval_sec: row.refresh_interval_sec.unwrap_or(300),
        enabled: row.enabled.unwrap_or(true),
        require_keywords: row.require_keywords.unwrap_or_default(),
        exclude_keywords: row.exclude_keywords.unwrap_or_default(),
    }))
}

async fn create_source(
    State(pool): State<PgPool>,
    Json(payload): Json<CreateSourceRequest>,
) -> Result<Json<SourceItem>, StatusCode> {
    // 简单校验
    if payload.name.is_empty() || payload.url.is_empty() || payload.feed_type.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    if !["rss", "atom", "api"].contains(&payload.feed_type.as_str()) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let row = sqlx::query!(
        r#"
        INSERT INTO sources (name, url, feed_type, refresh_interval_sec, enabled, require_keywords, exclude_keywords)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id, name, url, feed_type, refresh_interval_sec, enabled, require_keywords, exclude_keywords
        "#,
        payload.name,
        payload.url,
        payload.feed_type,
        payload.refresh_interval_sec.unwrap_or(300),
        payload.enabled.unwrap_or(true),
        &payload.require_keywords.unwrap_or_default(),
        &payload.exclude_keywords.unwrap_or_default()
    )
    .fetch_one(&pool)
    .await
    .map_err(|e| {
        error!("创建源失败: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(SourceItem {
        id: row.id,
        name: row.name,
        url: row.url,
        feed_type: row.feed_type,
        refresh_interval_sec: row.refresh_interval_sec.unwrap_or(300),
        enabled: row.enabled.unwrap_or(true),
        require_keywords: row.require_keywords.unwrap_or_default(),
        exclude_keywords: row.exclude_keywords.unwrap_or_default(),
    }))
}


// update_source 处理器
async fn update_source(
    State(pool): State<PgPool>,
    Path(id): Path<i32>,
    Json(payload): Json<CreateSourceRequest>, // 直接使用创建请求结构
) -> Result<StatusCode, StatusCode> {
    // 检查是否存在
    let exists = sqlx::query!("SELECT id FROM sources WHERE id = $1", id)
        .fetch_optional(&pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .is_some();
    if !exists {
        return Err(StatusCode::NOT_FOUND);
    }

    // 全量更新所有字段
    sqlx::query!(
        r#"
        UPDATE sources
        SET name = $1,
            url = $2,
            feed_type = $3,
            refresh_interval_sec = $4,
            enabled = $5,
            require_keywords = $6,
            exclude_keywords = $7
        WHERE id = $8
        "#,
        payload.name,
        payload.url,
        payload.feed_type,
        payload.refresh_interval_sec.unwrap_or(300),
        payload.enabled.unwrap_or(true),
        &payload.require_keywords.unwrap_or_default(),
        &payload.exclude_keywords.unwrap_or_default(),
        id
    )
    .execute(&pool)
    .await
    .map_err(|e| {
        error!("更新源失败: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(StatusCode::OK)
}


async fn delete_source(
    State(pool): State<PgPool>,
    Path(id): Path<i32>,
) -> Result<StatusCode, StatusCode> {
    let result = sqlx::query!("DELETE FROM sources WHERE id = $1", id)
        .execute(&pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if result.rows_affected() == 0 {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(StatusCode::OK)
}

/// 获取当前所有动态排除词（从所有源的 exclude_keywords 聚合）
async fn get_exclude_words(
    State(pool): State<PgPool>,
) -> Result<Json<Vec<String>>, StatusCode> {
    let rows = sqlx::query!(
        "SELECT DISTINCT unnest(exclude_keywords) as word FROM sources WHERE exclude_keywords IS NOT NULL AND array_length(exclude_keywords, 1) > 0"
    )
    .fetch_all(&pool)
    .await
    .map_err(|e| {
        error!("查询排除词失败: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let words = rows
        .into_iter()
        .filter_map(|r| r.word)
        .collect::<Vec<String>>();
    Ok(Json(words))
}