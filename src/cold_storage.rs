use sqlx::PgPool;
use tokio::time::{ Duration, sleep_until, Instant};
use tracing::{info, error};
use chrono::{Utc, Timelike};

pub async fn start_cold_storage_worker(pool: PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("启动冷热分离 Worker (每天凌晨 3:00 UTC 执行)");
    loop {
        // 计算下一次执行时间（凌晨 3:00）
        let now = Utc::now();
        let next_run = if now.hour() < 3 {
            now.with_hour(3).unwrap().with_minute(0).unwrap().with_second(0).unwrap()
        } else {
            (now + chrono::Duration::days(1)).with_hour(3).unwrap().with_minute(0).unwrap().with_second(0).unwrap()
        };
        let delay = (next_run - now).to_std().unwrap_or(Duration::from_secs(0));
        sleep_until(Instant::now() + delay).await;

        // 执行清理
        if let Err(e) = cleanup_old_news(&pool).await {
            error!("冷热分离清理失败: {}", e);
        } else {
            info!("冷热分离清理完成");
        }
    }
}

async fn cleanup_old_news(pool: &PgPool) -> Result<(), sqlx::Error> {
    let rows_affected = sqlx::query!(
        r#"
        UPDATE news
        SET content = NULL
        WHERE published_at < (NOW() AT TIME ZONE 'UTC' - INTERVAL '7 days')
          AND NOT EXISTS (
              SELECT 1 FROM prediction_tasks WHERE prediction_tasks.news_id = news.id
          )
        "#
    )
    .execute(pool)
    .await?
    .rows_affected();

    if rows_affected > 0 {
        info!("清空了 {} 条冷新闻的正文", rows_affected);
    }
    Ok(())
}