use axum::{extract::State,Json,Router,routing::get};
use serde::Serialize;
use sqlx::PgPool;
use chrono::{DateTime, Utc};
use bigdecimal::BigDecimal;


#[derive(Serialize)]
pub struct AntiFragileStats {
    asymmetry_ratio:BigDecimal,
    equity_curve:Vec<EquityPoint>,
    bubble_data:Vec<BubblePoint>,
}

#[derive(Serialize)]
pub struct EquityPoint{
    date:DateTime<Utc>,
    equal_weight_value:BigDecimal,
    position_weighted_value:BigDecimal,
}

#[derive(Serialize)]
pub struct BubblePoint{
    probability: BigDecimal,
    outcome: i32,
    position_size_pct: BigDecimal,
}


pub fn routes() -> Router<PgPool>{
    Router::new().route("/anti-fragile",get(get_anti_fragile_stats))
}

async fn get_anti_fragile_stats(
    State(pool): State<PgPool>,
) -> Json<AntiFragileStats>{
    let user_id = 1; //暂时固定用户id
    //1. 不对称比
    let asymmetry_ratio = compute_asymmetry_ratio(&pool,user_id).await
        .unwrap_or_else(|_|{BigDecimal::from(0)});
    //2. 虚拟净值曲线
    let equity_curve = compute_equity_curve(&pool,user_id).await
        .unwrap_or_else(|_|{vec![]});

        // 3. 气泡图数据
    let bubble_data = load_bubble_data(&pool, user_id).await
        .unwrap_or_else(|_| {vec![]});

    Json(AntiFragileStats {
        asymmetry_ratio,
        equity_curve,
        bubble_data,
    })
}

async fn compute_asymmetry_ratio(
    pool: &PgPool,
    user_id: i32
) -> Result<BigDecimal,sqlx::Error>{
    let rows = sqlx::query!(
        r#"
        SELECT position_size_pct, outcome
        FROM predictions
        WHERE user_id = $1
          AND prediction_type = 'human'
          AND outcome IS NOT NULL
          AND probability >= 50
        "#,
        user_id
    )
    .fetch_all(pool)
    .await?;

    let (correct_sum, correct_count, wrong_sum, wrong_count) = rows.iter().fold(
        (BigDecimal::from(0),0u32,BigDecimal::from(0),0u32), 
        |(c_sum, c_cnt, w_sum, w_cnt), row|{
            let pos = row.position_size_pct.clone();
            if row.outcome == Some(1){
                (c_sum+ pos,c_cnt +1,w_sum,w_cnt)
            }else {
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


async fn compute_equity_curve(pool: &PgPool, user_id: i32) -> Result<Vec<EquityPoint>, sqlx::Error> {
    // 获取所有已判决的预测，按时间排序（使用 submitted_at 作为时间轴）
    let rows = sqlx::query!(
        r#"
        SELECT submitted_at, outcome, position_size_pct
        FROM predictions
        WHERE user_id = $1
          AND prediction_type = 'human'
          AND outcome IS NOT NULL
        ORDER BY submitted_at ASC
        "#,
        user_id
    )
    .fetch_all(pool)
    .await?;

    let mut equal_weight = BigDecimal::from(1); // 初始净值1
    let mut position_weighted = BigDecimal::from(1);
    let mut points = Vec::new();

    for row in rows {
        let outcome = row.outcome.unwrap(); // 0 或 1
        let pos_pct = row.position_size_pct.clone();

        // 等权重：每次下注固定1单位，盈亏 ±1（简化）
        let equal_return = if outcome == 1 { BigDecimal::from(1) } else { BigDecimal::from(-1) };
        equal_weight += &equal_return;

        // 仓位加权：盈亏 = 仓位比例 * outcome（outcome 0为-1, 1为+1）
        let weighted_return = if outcome == 1 { pos_pct.clone() } else { -pos_pct.clone() };
        position_weighted += &weighted_return;

        points.push(EquityPoint {
            date: row.submitted_at.unwrap(),
            equal_weight_value: equal_weight.clone(),
            position_weighted_value: position_weighted.clone(),
        });
    }
    Ok(points)
}

async fn load_bubble_data(pool: &PgPool, user_id: i32) -> Result<Vec<BubblePoint>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT probability, outcome, position_size_pct
        FROM predictions
        WHERE user_id = $1
          AND prediction_type = 'human'
          AND outcome IS NOT NULL
        "#,
        user_id
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|row| BubblePoint {
        probability: row.probability,
        outcome: row.outcome.unwrap(),
        position_size_pct: row.position_size_pct,
    }).collect())
}