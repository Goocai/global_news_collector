use std::error::Error;
use sqlx::PgPool;
use std::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::{sync::Mutex,time};
use tracing::{info,  error};


use super::fetcher::fetch_and_store;
use crate::collector::DynamicExcludeWords;
use crate::collector::noise_analyzer::extract_noise_keywords;
/// 启动采集调度器：读取 sources 表，为每个启用的源启动一个独立任务，
/// 并根据 refresh_interval_sec 定期抓取。
/// 优化后的采集调度器
pub async fn start_collector_scheduler(
    pool: PgPool,
    dynamic_exclude: DynamicExcludeWords,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    info!("启动采集调度器（动态管理版本）");

    // 启动动态噪声词更新任务（每小时）
    let pool_clone = pool.clone();
    let exclude_clone = dynamic_exclude.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(3600));
        loop {
            interval.tick().await;
            if let Ok(keywords) = extract_noise_keywords(&pool_clone, 5).await {
                if !keywords.is_empty() {
                    let mut guard = exclude_clone.write().await;
                    for word in keywords {
                        guard.insert(word);
                    }
                    info!("动态排除词库已更新，当前共 {} 个词", guard.len());
                }
            }
        }
    });

    let pool = Arc::new(pool);
    let exclude = Arc::new(dynamic_exclude);
    let running_tasks = Arc::new(Mutex::new(HashMap::<i32, tokio::task::JoinHandle<()>>::new()));

    // 初始加载并启动所有启用源的任务
    let initial_sources = load_sources(&pool).await?;
    {
        let mut tasks = running_tasks.lock().await;
        for source in initial_sources {
            let source_id = source.id;  // 提前保存
            let handle = spawn_source_task(pool.clone(), source, exclude.clone());
            tasks.insert(source_id, handle);
        }
    }

    // 定期同步任务列表
    let mut interval = time::interval(Duration::from_secs(60));
    loop {
        interval.tick().await;

        match load_sources(&pool).await {
            Ok(new_sources) => {
                sync_tasks(&pool, &exclude, &running_tasks, new_sources).await;
            }
            Err(e) => {
                error!("重新加载源配置失败: {}", e);
            }
        }
    }

}

/// 启动单个源任务，内部加入错误恢复与日志
fn spawn_source_task(
    pool: Arc<PgPool>,
    source: SourceConfig,
    exclude: Arc<DynamicExcludeWords>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // 直接运行，run_source_task 内部会无限循环抓取，不会返回
        run_source_task(&pool, &source, &exclude).await;
    })
}

/// 根据新源列表，启动新任务、停止旧任务
async fn sync_tasks(
    pool: &Arc<PgPool>,
    exclude: &Arc<DynamicExcludeWords>,
    running_tasks: &Arc<Mutex<HashMap<i32, tokio::task::JoinHandle<()>>>>,
    new_sources: Vec<SourceConfig>,
) {
    let mut tasks = running_tasks.lock().await;

    // 收集新源 ID
    let new_ids: std::collections::HashSet<i32> = new_sources.iter().map(|s| s.id).collect();

    // 停止不再需要的任务
    let old_ids: Vec<i32> = tasks.keys().cloned().collect();
    for id in old_ids {
        if !new_ids.contains(&id) {
            if let Some(handle) = tasks.remove(&id) {
                handle.abort();
                info!("已停止源 {} 的采集任务", id);
            }
        }
    }

    // 启动新增的任务
    for source in new_sources {
        let source_id = source.id;
        if !tasks.contains_key(&source_id) {
            let handle = spawn_source_task(pool.clone(), source, exclude.clone());
            tasks.insert(source_id, handle);
            info!("已启动新源 {} 的采集任务", source_id);
        }
    }
}

/// 从数据库加载所有启用的源
async fn load_sources(pool: &PgPool) -> Result<Vec<SourceConfig>, Box<dyn Error+Send+Sync>> {
    let rows = sqlx::query!(
        r#"
        SELECT id, name, url, refresh_interval_sec 
        FROM sources 
        WHERE enabled = true
        "#
    )
    .fetch_all(pool)
    .await?;
   
   let mut sources = Vec::new();
   for row in rows {
       sources.push(SourceConfig{
           id: row.id,
           name: row.name,
           url: row.url,
           refresh_interval_sec: row.refresh_interval_sec.unwrap_or(300) as u64, // 默认5分钟
       });
   }
   Ok(sources)
}   

/// 单个源的任务：循环抓取，每次抓取后等待指定的间隔
async fn run_source_task(
    pool: &PgPool,                     // 改为 &PgPool
    source: &SourceConfig,             // 改为 &SourceConfig
    exclude_words: &DynamicExcludeWords,
) {
    let interval = Duration::from_secs(source.refresh_interval_sec);
    info!(
        "启动源任务: {}, ID: {}, 间隔: {}秒",
        source.name, source.id, source.refresh_interval_sec
    );

    loop {
        // 执行抓取
        if let Err(e) = fetch_and_store(pool, source.id, &source.url, exclude_words).await {
            error!("抓取源 {} 失败: {}", source.name, e);
        }

        // 等待下一次抓取
        time::sleep(interval).await;
    }
}


/// 源配置的内部结构
struct SourceConfig {
    id: i32,
    name: String,
    url: String,
    refresh_interval_sec: u64,
}