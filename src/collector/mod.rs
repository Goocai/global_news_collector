pub mod fetcher;
pub mod scheduler;
pub mod noise_analyzer;
// 导出子模块
pub use scheduler::start_collector_scheduler;


use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use sqlx::PgPool;


pub type DynamicExcludeWords = Arc<RwLock<HashSet<String>>>;

/// 初始化动态排除词库（从数据库加载已有排除词）
pub async fn init_dynamic_exclude_words(
    pool: &PgPool
) ->Result<DynamicExcludeWords,sqlx::Error>{
    let rows = sqlx::query!("SELECT exclude_keywords FROM sources WHERE enabled = true")
        .fetch_all(pool)
        .await?;
    let mut set = HashSet::new();
    for row in rows{
        if let Some(keywords) =  row.exclude_keywords{
            for kw in keywords{
                set.insert(kw);
            }
        }
    }

    Ok(Arc::new(RwLock::new(set)))
}
