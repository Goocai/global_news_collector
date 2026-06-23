pub mod news;
pub mod predictions;

use crate::stats;
// use axum::{Router, response::Html};
use axum::{Router,middleware};
use sqlx::PgPool;
// use std::path::PathBuf; 
mod post_mortem;
mod admin;
mod auth;

use crate::auth::middleware::auth_middleware;

pub fn routes(pool: PgPool) -> Router<PgPool> {
    // 受保护路由（需要认证）
    let protected = Router::new()
        .nest("/news", news::routes())
        .nest("/predictions", predictions::routes())
        .nest("/stats", stats::routers())
        .nest("/reviews", post_mortem::routes())
        .nest("/admin", admin::routes())
        .layer(middleware::from_fn_with_state(pool.clone(), auth_middleware));

    // 公开路由（无需认证）
    Router::new()
        .nest("/auth", auth::routes())   // 公开
        .merge(protected)                // 受保护
}
// pub async fn new_details_pages() ->Html<String>{
//     let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
//         .join("static/news_detail.html");
//     let content = tokio::fs::read_to_string(path)
//         .await
//         .unwrap_or_else(|_| "<h1>页面未找到</h1>".to_string());
//     Html(content)
// }

