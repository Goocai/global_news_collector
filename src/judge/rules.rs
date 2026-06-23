use async_trait::async_trait;
use serde_json::Value;
use sqlx::PgPool;
use std::error::Error;
use reqwest::Client;
use chrono::{ NaiveDate};
use std::env;

use crate::cache::{get_cached_response, set_cached_response};

/// 统一判定器错误类型（可扩展）
pub type RuleResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[async_trait]
pub trait RuleVerifier: Send + Sync {
    async fn verify(&self, rule: &Value, pool: &PgPool) -> RuleResult<bool>;
}

/// 价格涨跌幅判定器
pub struct PriceChangeVerifier;

#[async_trait]
impl RuleVerifier for PriceChangeVerifier {
    async fn verify(&self, rule: &Value, pool: &PgPool) -> RuleResult<bool> {
        let symbol = rule["symbol"].as_str().unwrap_or("AAPL");
        let base_date_str = rule["base_date"].as_str().unwrap_or("");
        let observation_date_str = rule["observation_date"].as_str().unwrap_or("");
        let threshold = rule["threshold_percent"].as_f64().unwrap_or(5.0);
        let direction = rule["direction"].as_str().unwrap_or("greater");

        // TODO: 调用 Yahoo Finance API
        let base_date = NaiveDate::parse_from_str(base_date_str, "%Y-%m-%d")
            .map_err(|e| format!("Invalid base_date: {}", e))?;
        let obs_date = NaiveDate::parse_from_str(observation_date_str, "%Y-%m-%d")
            .map_err(|e| format!("Invalid observation_date: {}", e))?;

        let base_price = get_close_price(pool, symbol, base_date).await?
            .ok_or_else(|| format!("No price data for {} on {}", symbol, base_date))?;
        let obs_price = get_close_price(pool, symbol, obs_date).await?
            .ok_or_else(|| format!("No price data for {} on {}", symbol, obs_date))?;

        let change = (obs_price - base_price) / base_price * 100.0;
        let satisfied = match direction {
            "greater" => change >= threshold,
            "less" => change <= -threshold,
            _ => false,
        };
        tracing::info!(
            "价格判定: {} 从 {} 到 {} 涨跌 {:.2}%, 阈值 {} {}, 满足: {}",
            symbol, base_price, obs_price, change, direction, threshold, satisfied
        );
        Ok(satisfied)
    }
}

/// 央行利率决议判定器
pub struct CentralBankVerifier;

#[async_trait]
impl RuleVerifier for CentralBankVerifier {
    async fn verify(&self, rule: &Value, pool: &PgPool) -> RuleResult<bool> {
        let bank = rule["bank"].as_str().unwrap_or("");
        let expected_action = rule["expected_action"].as_str().unwrap_or("");
        // let meeting_date = rule["meeting_date"].as_str().unwrap_or("");

                // 1. 映射规则中的 bank 字段到 FRED 的 series_id
        let series_id = match bank {
            "fomc" => "FEDFUNDS",      // 美联储联邦基金利率
            "ecb" => "ECBASSETSW",     // 欧洲央行主要再融资操作利率
            "pboc" => "CHNMLR",        // 中国人民银行贷款基础利率 (LPR)
            // 其他映射...
            _ => return Ok(false),      // 未知央行，直接返回不满足
        };

        // 2. FRED API 请求
        // 3. 解析最新利率值
        let current_rate = get_fred_series_value(pool, series_id).await?
            .ok_or_else(|| format!("No rate data for {} on fred",series_id))?;

        // 4. 判定是否满足预期
        let expected_rate: f64 = expected_action.parse().unwrap_or(0.0);
        let satisfied = (current_rate - expected_rate).abs() < f64::EPSILON;

        tracing::info!(
            "央行利率判定: 预期 {}% 实际 {}% -> {}",
            expected_rate, current_rate, satisfied
        );
        Ok(satisfied)
    }
}

/// 经济数据对比判定器
pub struct EconomicDataVerifier;

#[async_trait]
impl RuleVerifier for EconomicDataVerifier {
    async fn verify(&self, rule: &Value, pool: &PgPool) -> RuleResult<bool> {

        let indicator = rule["indicator"].as_str().unwrap_or("");
        let operator = rule["operator"].as_str().unwrap_or("greater");
        let expected = rule["expected_value"].as_f64().unwrap_or(0.0);

        // 1. 映射指标名称到 FRED series_id
        let series_id = match indicator {
            "cpi" => "CPIAUCSL",        // 美国CPI
            "gdp" => "GDP",              // 美国GDP
            "unemployment" => "UNRATE",  // 美国失业率
            // 其他映射...
            _ => return Ok(false),
        };

        // 2. 请求 FRED API 获取最新值
        let actual = get_fred_series_value(pool, series_id).await?
            .ok_or_else(|| format!("No rate data for {} on fred",series_id))?;

        // 3. 判定
        let satisfied = match operator {
            "greater" => actual > expected,
            "less" => actual < expected,
            "equal" => (actual - expected).abs() < 1e-6,
            _ => false,
        };

        tracing::info!("经济数据判定: {} (预期 {} {} 实际 {}) -> {}", indicator, expected, operator, actual, satisfied);
        Ok(satisfied)

    }
}

/// URL关键词检测
pub struct UrlKeywordVerifier;

#[async_trait]
impl RuleVerifier for UrlKeywordVerifier {
    async fn verify(&self, rule: &Value, _pool: &PgPool) -> RuleResult<bool> {
        let _url = rule["url"].as_str().unwrap_or("");
        let empty_vec = vec![];
        let keywords = rule["keywords"].as_array().unwrap_or(&empty_vec);
        let match_all = rule["match_all"].as_bool().unwrap_or(false);
        // 模拟获取网页内容
        let content = "<html>example</html>";
        let contains = keywords.iter().all(|k| content.contains(k.as_str().unwrap_or("")));
        let result = if match_all { contains } else { keywords.iter().any(|k| content.contains(k.as_str().unwrap_or(""))) };
        Ok(result)
    }
}



/// 获取指定股票在某个日期的收盘价（美股）
/// async function getHistoricalData(symbol, period1, period2) {
//   const history = await yahooFinance.historical(symbol, {
//     period1, // Start date
//     period2, // End date
//     interval: '1d' // '1d', '1wk', '1mo'
//   });
/// 
async fn get_close_price(pool: &PgPool, symbol: &str, date: NaiveDate) -> Result<Option<f64>, Box<dyn std::error::Error + Send + Sync>> {
    
    //1. 构造缓存键
    let cache_key = format!("yahoo:{}:{}", symbol, date);
    // 2. 尝试从缓存读取
    if let Some(cached) = get_cached_response(pool, &cache_key).await? {
        if let Some(price) = cached.as_f64() {
            return Ok(Some(price));
        }
    }
    // 3. 缓存未命中，调用真实 API  
    // 设置 User-Agent 避免被拒
    let client = Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        .build()?;

    // 时间戳：当天 00:00:00 UTC 到下一天 00:00:00 UTC
    let start = date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();
    let end = date.succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();

    let url = format!(
        "https://query1.finance.yahoo.com/v8/finance/chart/{}?period1={}&period2={}&interval=1d",
        symbol, start, end
    );

    let resp = client.get(&url).send().await?;
    let json: Value = resp.json().await?;

    // 解析收盘价（可能为 null）
    let close_prices = json["chart"]["result"][0]["indicators"]["quote"][0]["close"].as_array();
    if let Some(prices) = close_prices {
        if let Some(price) = prices.get(0).and_then(|p| p.as_f64()) {
            return Ok(Some(price));
        }
    }
    Ok(None)
}


async fn get_fred_series_value(
    pool:&PgPool,
    series_id:  &str,
) -> Result<Option<f64>, Box<dyn Error+Send+Sync>>{
    let cache_key = format!("fred:{}", series_id);
    if let Some(cached) = get_cached_response(pool, &cache_key).await? {
        if let Some(value) = cached["value"].as_f64() {
            return Ok(Some(value));
        }
    }

    // 2. 请求 FRED API 获取最新值
    let api_key = env::var("FRED_API_KEY").expect("FRED_API_KEY must be set");
    let url = format!(
        "https://api.stlouisfed.org/fred/series/observations?series_id={}&api_key={}&file_type=json&sort_order=desc&limit=1",
        series_id, api_key
    );
    let client = Client::new();
    let response = client.get(&url).send().await?;
    let json: Value = response.json().await?;

    let value = json["observations"][0]["value"].as_str()
        .and_then(|s| s.parse().ok());

    let cache_value = serde_json::json!({ "value": value });
    let _ = set_cached_response(pool, &cache_key, &cache_value, 3600).await;
    Ok(value)
}