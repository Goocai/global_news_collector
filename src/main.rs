use axum::Router;
use tokio::net::TcpListener;
use std::net::SocketAddr;
use tracing_subscriber;
use sqlx::postgres::PgPoolOptions;
use tower_http::services::ServeDir;
use tracing::{info, error};

mod api;
mod collector;
mod judge;
mod llm;
mod stats; 
mod cold_storage;
mod cache;
mod auth;

use cache::clean_expired_cache;
use collector::init_dynamic_exclude_words;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    dotenvy::dotenv().ok();
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    info!("数据库连接成功");

    let dynamic_exclude = init_dynamic_exclude_words(&pool).await?;
    let pool_clone = pool.clone();
    let exclude_clone = dynamic_exclude.clone();
    
    tokio::spawn(async move {
        if let Err(e) = collector::start_collector_scheduler(pool_clone,exclude_clone).await {
            error!("采集调度器出错: {}", e);
        }
    });

    // 启动判定状态机 Worker
    let pool_judge = pool.clone();
    tokio::spawn(async move {
        if let Err(e) = judge::start_judge_worker(pool_judge).await{
            error!("判定状态机 Worker 异常: {}", e)
        }
    });

    // 启动 LLM Worker
    let pool_llm = pool.clone();
    tokio::spawn(async move {
        if let Err(e) = llm::start_llm_worker(pool_llm).await {
            error!("LLM Worker 异常: {}", e);
        }
    });


    // 启动 Brier 快照定时任务
    let pool_brier = pool.clone();
    tokio::spawn(async move {
        if let Err(e) = stats::brier::start_brier_scheduler(&pool_brier).await {
            error!("Brier 定时任务异常: {}", e);
        }
    });

    // 启动冷热分离 Worker
    let pool_cold = pool.clone();
    tokio::spawn(async move {
        if let Err(e) = cold_storage::start_cold_storage_worker(pool_cold).await {
            error!("冷热分离 Worker 异常: {}", e);
        }
    });


    //定时清理过期缓存 api 请求
    let pool_clean = pool.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(3600));
        loop {
            interval.tick().await;
            if let Ok(count) = clean_expired_cache(&pool_clean).await {
                if count > 0 {
                    tracing::info!("清理了 {} 条过期缓存", count);
                }
            }
        }
    });

    // 构建路由：API + 静态文件服务（作为 fallback）
    let app = Router::new()
        .nest("/api", api::routes(pool.clone()))   // 所有 API 统一在 /api 下
        .fallback_service(ServeDir::new("static")) // 静态文件服务
        .with_state(pool);

    
    let addr = SocketAddr::from(([127, 0, 0, 1], 8000));
    let listener = TcpListener::bind(addr).await?;
    info!("Web服务监听: http://{}", addr);
    axum::serve(listener, app).await?;
    Ok(())
}