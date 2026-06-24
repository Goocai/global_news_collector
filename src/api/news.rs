use axum::{
    extract::{Path, State, Query},
    http::StatusCode,
    response::Json,
    Router,
    routing::{get,post},
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool};
use chrono::{DateTime, Utc};
use crate::auth::extractor::AuthUser;

// 列表项的响应结构
#[derive(Serialize)]
pub struct NewsListItem{
    id:i32,
    title:String,
    source_name:String,
    published_at:DateTime<Utc>,
    summary:String,
}



// 详情响应结构
#[derive(Serialize)]
pub struct NewsDetail {
    pub id: i32,
    pub title: String,
    pub summary: Option<String>,
    pub source_name: String,
    pub published_at: DateTime<Utc>,
    pub url: String,
}

// 分页参数
#[derive(Deserialize)]
pub struct Pagination {
    page: Option<u32>,
    per_page: Option<u32>,
}

pub fn routes() -> Router<PgPool> {
    Router::new()
        .route("/", get(list_news))
        .route("/{id}", get(get_news))
        .route("/{id}/noise", post(mark_noise))
}

// 新闻列表
async fn list_news(
    State(pool): State<PgPool>,
    Query(pagination): Query<Pagination>,
) -> Result<Json<Vec<NewsListItem>>, StatusCode> {
    let page = pagination.page.unwrap_or(1);
    let per_page = pagination.per_page.unwrap_or(20).min(100);
    let offset = (page - 1) * per_page;

    let rows= sqlx::query!(
            r#"
            SELECT 
                n.id, 
                n.title, 
                s.name as source_name, 
                n.published_at,
                COALESCE(n.description, n.title) as summary
            FROM news n
            JOIN sources s ON n.source_id = s.id
            ORDER BY n.published_at DESC
            LIMIT $1 OFFSET $2
            "#,
            per_page as i32,
            offset as i32
        )
        .fetch_all(&pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    
    let items = rows.into_iter().map(|row| NewsListItem {
        id:row.id,
        title:row.title,
        source_name:row.source_name,
        published_at:row.published_at,
        summary: row.summary.unwrap_or_default(),
    }).collect();

    Ok(Json(items))
}


// 新闻详情
async fn get_news(
    State(pool): State<PgPool>,
    Path(id): Path<i32>,
) -> Result<Json<NewsDetail>, StatusCode> {
    let row = sqlx::query!(
        r#"
        SELECT 
            n.id, 
            n.title, 
            n.description,
            n.url,
            s.name as source_name, 
            n.published_at
        FROM news n
        JOIN sources s ON n.source_id = s.id
        WHERE n.id = $1
        "#,
        id
    )
    .fetch_one(&pool)
    .await
    .map_err(|err| {
        if let sqlx::Error::RowNotFound = err {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    })?;


    Ok(Json(NewsDetail {
        id: row.id,
        title: row.title,
        summary: row.description,
        source_name: row.source_name,
        published_at: row.published_at,
        url: row.url,
    }))
    
}


async fn mark_noise(
    State(pool): State<PgPool>,
    auth_user: AuthUser,
    Path(id): Path<i32>
) ->  StatusCode {
    let user_id = auth_user.user_id;
    let result = sqlx::query!(
        r#"
        INSERT INTO noise_flags (news_id , user_id) VALUES ($1 , $2) ON  CONFLICT  DO NOTHING
        "#,
        id,user_id
    )
    .execute(&pool)
    .await;

    match result {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}