use axum::{extract::State, Json, Router, routing::get};
use serde::Serialize;
use sqlx::PgPool;
use chrono::{DateTime, Utc};
use bigdecimal::BigDecimal;

// ============ 数据结构 ============
#[derive(Serialize)]
pub struct AntiFragileStats {
    human: AgentStats,
    llm: AgentStats,
}

#[derive(Serialize)]
pub struct AgentStats {
    asymmetry_ratio: BigDecimal,
    equity_curve: Vec<EquityPoint>,
    bubble_data: Vec<BubblePoint>,
}

#[derive(Serialize)]
pub struct EquityPoint {
    date: DateTime<Utc>,
    equal_weight_value: BigDecimal,
    position_weighted_value: BigDecimal,
}

#[derive(Serialize)]
pub struct BubblePoint {
    probability: BigDecimal,
    outcome: i32,
    position_size_pct: BigDecimal,
}

// ============ 路由 ============
pub fn routes() -> Router<PgPool> {
    Router::new().route("/anti-fragile", get(get_anti_fragile_stats))
}

// ============ 主处理函数 ============
async fn get_anti_fragile_stats(
    State(pool): State<PgPool>,
) -> Json<AntiFragileStats> {
    let user_id = 1; // 暂时固定用户id

    // 并行计算人类和 LLM 的三项指标
    let (human_asym, llm_asym, human_equity, llm_equity, human_bubble, llm_bubble) = tokio::join!(
        compute_asymmetry_ratio(&pool, user_id),
        compute_asymmetry_ratio_llm(&pool, user_id),
        compute_equity_curve(&pool, user_id),
        compute_equity_curve_llm(&pool, user_id),
        load_bubble_data(&pool, user_id),
        load_bubble_data_llm(&pool, user_id),
    );

    Json(AntiFragileStats {
        human: AgentStats {
            asymmetry_ratio: human_asym.unwrap_or_else(|_| BigDecimal::from(0)),
            equity_curve: human_equity.unwrap_or_else(|_| vec![]),
            bubble_data: human_bubble.unwrap_or_else(|_| vec![]),
        },
        llm: AgentStats {
            asymmetry_ratio: llm_asym.unwrap_or_else(|_| BigDecimal::from(0)),
            equity_curve: llm_equity.unwrap_or_else(|_| vec![]),
            bubble_data: llm_bubble.unwrap_or_else(|_| vec![]),
        },
    })
}

// ============ 人类统计计算 ============
async fn compute_asymmetry_ratio(
    pool: &PgPool,
    user_id: i32,
) -> Result<BigDecimal, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT p.id, p.outcome_human as outcome, hp.position_size_pct
        FROM prediction_tasks p
        JOIN human_predictions hp ON p.id = hp.task_id
        WHERE p.user_id = $1
          AND p.outcome_human IS NOT NULL
          AND hp.probability >= 50
        "#,
        user_id
    )
    .fetch_all(pool)
    .await?;

    let (correct_sum, correct_count, wrong_sum, wrong_count) = rows.iter().fold(
        (BigDecimal::from(0), 0u32, BigDecimal::from(0), 0u32),
        |(c_sum, c_cnt, w_sum, w_cnt), row| {
            let pos = row.position_size_pct.clone();
            if row.outcome == Some(1) {
                (c_sum + pos, c_cnt + 1, w_sum, w_cnt)
            } else {
                (c_sum, c_cnt, w_sum + pos, w_cnt + 1)
            }
        },
    );

    if correct_count == 0 || wrong_count == 0 {
        return Ok(BigDecimal::from(0));
    }
    let correct_avg = correct_sum / BigDecimal::from(correct_count);
    let wrong_avg = wrong_sum / BigDecimal::from(wrong_count);
    if wrong_avg == BigDecimal::from(0) {
        return Ok(BigDecimal::from(0));
    }
    Ok(correct_avg / wrong_avg)
}

async fn compute_equity_curve(
    pool: &PgPool,
    user_id: i32,
) -> Result<Vec<EquityPoint>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT p.submitted_at, p.outcome_human as outcome, hp.position_size_pct
        FROM prediction_tasks p
        JOIN human_predictions hp ON p.id = hp.task_id
        WHERE p.user_id = $1
          AND p.outcome_human IS NOT NULL
        ORDER BY p.submitted_at ASC
        "#,
        user_id
    )
    .fetch_all(pool)
    .await?;

    let mut equal_weight = BigDecimal::from(1);
    let mut position_weighted = BigDecimal::from(1);
    let mut points = Vec::new();

    for row in rows {
        let outcome = row.outcome.unwrap(); // 0 或 1
        let pos_pct = row.position_size_pct.clone();

        let equal_return = if outcome == 1 {
            BigDecimal::from(1)
        } else {
            BigDecimal::from(-1)
        };
        equal_weight += &equal_return;

        let weighted_return = if outcome == 1 {
            pos_pct.clone()
        } else {
            -pos_pct.clone()
        };
        position_weighted += &weighted_return;

        points.push(EquityPoint {
            date: row.submitted_at.unwrap(),
            equal_weight_value: equal_weight.clone(),
            position_weighted_value: position_weighted.clone(),
        });
    }
    Ok(points)
}

async fn load_bubble_data(
    pool: &PgPool,
    user_id: i32,
) -> Result<Vec<BubblePoint>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT hp.probability, p.outcome_human as outcome, hp.position_size_pct
        FROM prediction_tasks p
        JOIN human_predictions hp ON p.id = hp.task_id
        WHERE p.user_id = $1
          AND p.outcome_human IS NOT NULL
        "#,
        user_id
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| BubblePoint {
            probability: row.probability,
            outcome: row.outcome.unwrap(),
            position_size_pct: row.position_size_pct,
        })
        .collect())
}

// ============ LLM 统计计算（复制并修改表名/列名） ============
async fn compute_asymmetry_ratio_llm(
    pool: &PgPool,
    user_id: i32,
) -> Result<BigDecimal, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT p.id, p.outcome_llm as outcome, lp.position_size_pct
        FROM prediction_tasks p
        JOIN llm_predictions lp ON p.id = lp.task_id
        WHERE p.user_id = $1
          AND p.outcome_llm IS NOT NULL
          AND lp.probability >= 50
        "#,
        user_id
    )
    .fetch_all(pool)
    .await?;

    let (correct_sum, correct_count, wrong_sum, wrong_count) = rows.iter().fold(
        (BigDecimal::from(0), 0u32, BigDecimal::from(0), 0u32),
        |(c_sum, c_cnt, w_sum, w_cnt), row| {
            let pos = row.position_size_pct.clone();
            if row.outcome == Some(1) {
                (c_sum + pos, c_cnt + 1, w_sum, w_cnt)
            } else {
                (c_sum, c_cnt, w_sum + pos, w_cnt + 1)
            }
        },
    );

    if correct_count == 0 || wrong_count == 0 {
        return Ok(BigDecimal::from(0));
    }
    let correct_avg = correct_sum / BigDecimal::from(correct_count);
    let wrong_avg = wrong_sum / BigDecimal::from(wrong_count);
    if wrong_avg == BigDecimal::from(0) {
        return Ok(BigDecimal::from(0));
    }
    Ok(correct_avg / wrong_avg)
}

async fn compute_equity_curve_llm(
    pool: &PgPool,
    user_id: i32,
) -> Result<Vec<EquityPoint>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT p.submitted_at, p.outcome_llm as outcome, lp.position_size_pct
        FROM prediction_tasks p
        JOIN llm_predictions lp ON p.id = lp.task_id
        WHERE p.user_id = $1
          AND p.outcome_llm IS NOT NULL
        ORDER BY p.submitted_at ASC
        "#,
        user_id
    )
    .fetch_all(pool)
    .await?;

    let mut equal_weight = BigDecimal::from(1);
    let mut position_weighted = BigDecimal::from(1);
    let mut points = Vec::new();

    for row in rows {
        let outcome = row.outcome.unwrap();
        let pos_pct = row.position_size_pct.clone();

        let equal_return = if outcome == 1 {
            BigDecimal::from(1)
        } else {
            BigDecimal::from(-1)
        };
        equal_weight += &equal_return;

        let weighted_return = if outcome == 1 {
            pos_pct.clone()
        } else {
            -pos_pct.clone()
        };
        position_weighted += &weighted_return;

        points.push(EquityPoint {
            date: row.submitted_at.unwrap(),
            equal_weight_value: equal_weight.clone(),
            position_weighted_value: position_weighted.clone(),
        });
    }
    Ok(points)
}

async fn load_bubble_data_llm(
    pool: &PgPool,
    user_id: i32,
) -> Result<Vec<BubblePoint>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT lp.probability, p.outcome_llm as outcome, lp.position_size_pct
        FROM prediction_tasks p
        JOIN llm_predictions lp ON p.id = lp.task_id
        WHERE p.user_id = $1
          AND p.outcome_llm IS NOT NULL
        "#,
        user_id
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| BubblePoint {
            probability: row.probability,
            outcome: row.outcome.unwrap(),
            position_size_pct: row.position_size_pct,
        })
        .collect())
}