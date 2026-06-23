use sqlx::PgPool;
use serde_json::Value;
use chrono::{Duration, Utc};

/// 从缓存中获取数据
pub async fn get_cached_response(
    pool: &PgPool,
    key: &str,
) -> Result<Option<Value>, sqlx::Error> {
    let row = sqlx::query!(
        "SELECT response_data FROM api_cache WHERE cache_key = $1 AND expires_at > NOW()",
        key
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.response_data))
}

/// 将响应存入缓存，默认过期时间为 1 小时
pub async fn set_cached_response(
    pool: &PgPool,
    key: &str,
    data: &Value,
    ttl_seconds: i64,
) -> Result<(), sqlx::Error> {
    let expires_at = Utc::now() + Duration::seconds(ttl_seconds);
    sqlx::query!(
        r#"
        INSERT INTO api_cache (cache_key, response_data, expires_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (cache_key) DO UPDATE
        SET response_data = $2, expires_at = $3, created_at = NOW()
        "#,
        key,
        data,
        expires_at
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// 清理过期缓存（定时任务）
pub async fn clean_expired_cache(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let result = sqlx::query!("DELETE FROM api_cache WHERE expires_at <= NOW()")
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}