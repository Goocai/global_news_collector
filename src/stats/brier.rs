use axum::{extract::State, Json, Router, routing::get};
use serde::Serialize;                      
use sqlx::PgPool;
use chrono::{DateTime, Utc, Datelike};   
use bigdecimal::BigDecimal;    
use tokio::time::{interval, Duration, sleep};

#[derive(Serialize)]
pub struct BrierHistoryItem {
    calculation_time: DateTime<Utc>,
    human_brier: BigDecimal,
    llm_brier: BigDecimal,
    delta: BigDecimal,
}

// 修正路由：.nest() 需要 Router，应使用 .route()
pub fn routes() -> Router<PgPool> {
    Router::new().route("/stats", get(get_brier_history))
}

async fn get_brier_history(
    State(pool): State<PgPool>,
) -> Json<Vec<BrierHistoryItem>> {
    let user_id = 1;
    let rows = sqlx::query!(
        r#"
        SELECT calculation_time, human_brier, llm_brier, delta
        FROM brier_history
        WHERE user_id = $1
        ORDER BY calculation_time ASC
        "#,
        user_id
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_else(|_| vec![]);

    let items = rows
        .into_iter()
        .map(|r| BrierHistoryItem {
            calculation_time: r.calculation_time.unwrap(),
            human_brier: r.human_brier,
            llm_brier: r.llm_brier,
            delta: r.delta.unwrap(),       // 生成列，保证非空
        })
        .collect();
    Json(items)
}

pub async fn compute_brier_scores(
    pool: &PgPool,
    user_id: i32,
) -> Result<(BigDecimal, BigDecimal), sqlx::Error> {
    // 人类预测
    let human = sqlx::query!(
        r#"
        SELECT probability, outcome
        FROM predictions
        WHERE user_id = $1 AND prediction_type = 'human' AND outcome IS NOT NULL
        "#,
        user_id
    )
    .fetch_all(pool)
    .await?;

    // 使用 BigDecimal 运算，避免 f64 精度丢失
    let human_brier = if human.is_empty() {
        BigDecimal::from(0)
    } else {
        let sum: BigDecimal = human
            .iter()
            .map(|r| {
                // probability 是非空 BigDecimal，outcome 已过滤 IS NOT NULL，直接 unwrap 即可
                let p = r.probability.clone() / BigDecimal::from(100);
                let o = BigDecimal::from(r.outcome.unwrap()); // outcome: i32
                let diff = p - o;
                diff.clone() * diff
            })
            .sum();
        sum / BigDecimal::from(human.len() as u64)
    };

    // LLM 预测
    let llm = sqlx::query!(
        r#"
        SELECT p.probability, p.outcome
        FROM predictions p
        JOIN predictions parent ON p.parent_prediction_id = parent.id
        WHERE parent.user_id = $1 AND p.outcome IS NOT NULL
        "#,
        user_id
    )
    .fetch_all(pool)
    .await?;

    let llm_brier = if llm.is_empty() {
        BigDecimal::from(0)
    } else {
        let sum: BigDecimal = llm
            .iter()
            .map(|r| {
                let p = r.probability.clone() / BigDecimal::from(100);
                let o = BigDecimal::from(r.outcome.unwrap());
                let diff = p - o;
                diff.clone() * diff
            })
            .sum();
        sum / BigDecimal::from(human.len() as u64)
    };

    Ok((human_brier, llm_brier))
}

pub async fn daily_brier_snapshot(pool: &PgPool) -> Result<(), sqlx::Error> {
    let users = sqlx::query!(
        "SELECT DISTINCT user_id FROM predictions WHERE user_id IS NOT NULL"
    )
    .fetch_all(pool)
    .await?;

    for user in users {
        if let Some(user_id) = user.user_id {
            let (human_brier, llm_brier) = compute_brier_scores(pool, user_id).await?;
            // 不对称比占位，Step 11 会填充真实值
            let asymmetry_ratio = BigDecimal::from(0); // 直接由整数构造
            sqlx::query!(
                r#"
                INSERT INTO brier_history (user_id, human_brier, llm_brier, asymmetry_ratio)
                VALUES ($1, $2, $3, $4)
                "#,
                user_id,
                human_brier,
                llm_brier,
                asymmetry_ratio
            )
            .execute(pool)
            .await?;
        }
    }
    Ok(())
}

pub async fn start_brier_scheduler(
    pool: &PgPool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut clock_interval = interval(Duration::from_secs(24 * 3600));
    let now = Utc::now();
    let tomorrow_midnight = chrono::NaiveDate::from_ymd_opt(
        now.year(),   // 需要 Datelike
        now.month(),
        now.day(),
    )
    .unwrap()
    .succ_opt()
    .unwrap()
    .and_hms_opt(0, 0, 0)
    .unwrap();
    let delay = (tomorrow_midnight - now.naive_utc())
        .to_std()
        .unwrap_or(Duration::from_secs(0));
    sleep(delay).await;
    loop {
        clock_interval.tick().await;
        if let Err(e) = daily_brier_snapshot(pool).await {
            tracing::error!("Brier 快照任务失败: {}", e);
        } else {
            tracing::info!("Brier 快照任务完成");
        }
    }
}