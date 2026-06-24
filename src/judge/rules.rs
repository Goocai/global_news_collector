use serde_json::Value;
use sqlx::PgPool;
use std::error::Error;
use reqwest::{Client,Url};
use chrono::{NaiveDate, Duration, Datelike};  // 添加 Datelike
use std::env;
use serde::{Deserialize, Serialize};
use tracing::info;   // 只保留 info
use std::sync::{Arc, LazyLock};  // 使用 LazyLock
use tokio::sync::Mutex;
use regex::Regex;

use crate::cache::{get_cached_response, set_cached_response};

// ========== 规则枚举定义 ==========
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Rule {
    #[serde(rename = "price_change")]
    PriceChange {
        symbol: String,
        market: String,
        base_date: NaiveDate,
        observation_date: NaiveDate,
        threshold_percent: f64,
        direction: String,
    },

    #[serde(rename = "central_bank")]
    CentralBank {
        bank: String,
        meeting_date: NaiveDate,
        expected_action: String,
    },

    #[serde(rename = "url_keyword")]
    UrlKeyword {
        url: String,
        keywords: Vec<String>,
        match_all: bool,
    },

    #[serde(rename = "economic_data")]
    EconomicData {
        indicator: String,
        country: String,
        release_date: NaiveDate,
        expected_value: f64,
        operator: String,
        actual_value_source: String,
        actual_value: Option<f64>,
    },

    #[serde(rename = "earnings")]
    Earnings {
        symbol: String,
        market: String,
        quarter: String,
        expected_eps: f64,
        actual_eps_source: String,
        actual_eps: Option<f64>,
    },

    #[serde(rename = "commodity")]
    Commodity {
        symbol: String,
        market: String,
        base_date: NaiveDate,
        observation_date: NaiveDate,
        threshold_percent: f64,
        direction: String,
    },
}

// ========== 核心判定函数 ==========
pub async fn evaluate_rule(rule: &Rule, pool: &PgPool) -> Result<bool, Box<dyn Error + Send + Sync>> {
    match rule {
        Rule::PriceChange { symbol, base_date, observation_date, threshold_percent, direction, .. } => {
            let change = fetch_price_change(pool, symbol, base_date, observation_date).await?;
            let satisfied = match direction.as_str() {
                "greater" => change >= *threshold_percent,
                "less" => change <= *threshold_percent,
                _ => false,
            };
            info!(
                "价格判定: {} 涨跌幅 {:.2}% (阈值 {} {}) -> {}",
                symbol, change, direction, threshold_percent, satisfied
            );
            Ok(satisfied)
        }

        Rule::CentralBank { bank, meeting_date, expected_action } => {
            let actual = fetch_central_bank_action(pool, bank, meeting_date).await?;
            let satisfied = actual == *expected_action;
            info!(
                "央行判定: {} 预期 {} 实际 {} -> {}",
                bank, expected_action, actual, satisfied
            );
            Ok(satisfied)
        }

        Rule::UrlKeyword { url, keywords, match_all } => {
            let content = fetch_url_content(url).await?;
            let result = if *match_all {
                keywords.iter().all(|kw| content.contains(kw.as_str()))
            } else {
                keywords.iter().any(|kw| content.contains(kw.as_str()))
            };
            info!("URL关键词判定: {} -> {}", url, result);
            Ok(result)
        }

        Rule::EconomicData { indicator, expected_value, operator, actual_value_source, actual_value, .. } => {
            let actual = match actual_value_source.as_str() {
                "manual" => actual_value.ok_or("Missing manual value")?,
                "api" => fetch_economic_data(pool, indicator).await?,
                _ => return Err("Invalid actual_value_source".into()),
            };
            let satisfied = match operator.as_str() {
                "greater" => actual > *expected_value,
                "less" => actual < *expected_value,
                "equal" => (actual - expected_value).abs() < 1e-6,
                _ => false,
            };
            info!("经济数据判定: {} 预期 {} 实际 {} -> {}", indicator, expected_value, actual, satisfied);
            Ok(satisfied)
        }

        Rule::Earnings { symbol, expected_eps, actual_eps_source, actual_eps, .. } => {
            let actual = match actual_eps_source.as_str() {
                "manual" => actual_eps.ok_or("Missing manual EPS")?,
                "api" => fetch_earnings(pool, symbol).await?,
                _ => return Err("Invalid actual_eps_source".into()),
            };
            let satisfied = actual > *expected_eps;
            info!("财报判定: {} 预期 EPS {} 实际 {} -> {}", symbol, expected_eps, actual, satisfied);
            Ok(satisfied)
        }

        Rule::Commodity { symbol, base_date, observation_date, threshold_percent, direction, .. } => {
            let change = fetch_commodity_change(pool, symbol, base_date, observation_date).await?;
            let satisfied = match direction.as_str() {
                "greater" => change >= *threshold_percent,
                "less" => change <= *threshold_percent,
                _ => false,
            };
            info!("商品判定: {} 涨跌幅 {:.2}% (阈值 {} {}) -> {}", symbol, change, direction, threshold_percent, satisfied);
            Ok(satisfied)
        }
    }
}

// ========== 辅助数据获取函数（带缓存） ==========

async fn fetch_price_change(
    pool: &PgPool,
    symbol: &str,
    base_date: &NaiveDate,
    obs_date: &NaiveDate,
) -> Result<f64, Box<dyn Error + Send + Sync>> {
    let base_price = get_close_price(pool, symbol, base_date).await?
        .ok_or_else(|| format!("No price data for {} on {}", symbol, base_date))?;
    let obs_price = get_close_price(pool, symbol, obs_date).await?
        .ok_or_else(|| format!("No price data for {} on {}", symbol, obs_date))?;
    Ok((obs_price - base_price) / base_price * 100.0)
}

async fn fetch_commodity_change(
    pool: &PgPool,
    symbol: &str,
    base_date: &NaiveDate,
    obs_date: &NaiveDate,
) -> Result<f64, Box<dyn Error + Send + Sync>> {
    fetch_price_change(pool, symbol, base_date, obs_date).await
}

async fn fetch_central_bank_action(
    pool: &PgPool,
    bank: &str,
    meeting_date: &NaiveDate,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let series_id = match bank {
        "fomc" => "FEDFUNDS",
        "ecb" => "ECBASSETSW",
        "boj" => "JPNIRLTR",
        "pboc" => "CHNMLR",
        _ => return Err(format!("Unsupported bank: {}", bank).into()),
    };

    let rate_meeting = get_fred_rate_on_date(pool, series_id, meeting_date).await?;
    let prev_date = *meeting_date - Duration::days(1);
    let rate_prev = get_fred_rate_on_date(pool, series_id, &prev_date).await?;

    let diff_bps = (rate_meeting - rate_prev) * 100.0;
    let action = if diff_bps.abs() < 0.5 {
        "hold".to_string()
    } else if diff_bps > 0.0 {
        format!("hike_{:.0}bp", diff_bps)
    } else {
        format!("cut_{:.0}bp", -diff_bps)
    };
    Ok(action)
}

async fn get_fred_rate_on_date(
    pool: &PgPool,
    series_id: &str,
    date: &NaiveDate,
) -> Result<f64, Box<dyn Error + Send + Sync>> {
    let cache_key = format!("fred:{}:{}", series_id, date);
    if let Some(cached) = get_cached_response(pool, &cache_key).await? {
        if let Some(value) = cached["value"].as_f64() {
            return Ok(value);
        }
    }

    let api_key = env::var("FRED_API_KEY").map_err(|_| "FRED_API_KEY not set")?;
    let date_str = date.format("%Y-%m-%d").to_string();
    let url = format!(
        "https://api.stlouisfed.org/fred/series/observations?series_id={}&api_key={}&file_type=json&observation_date={}",
        series_id, api_key, date_str
    );

    let client = Client::new();
    let resp = client.get(&url).send().await?;
    let json: Value = resp.json().await?;

    let value = json["observations"][0]["value"]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| format!("No data for {} on {}", series_id, date))?;

    let cache_value = serde_json::json!({ "value": value });
    set_cached_response(pool, &cache_key, &cache_value, 3600).await?;
    Ok(value)
}

async fn fetch_economic_data(pool: &PgPool, indicator: &str) -> Result<f64, Box<dyn Error + Send + Sync>> {
    let series_id = match indicator {
        "cpi" => "CPIAUCSL",
        "gdp" => "GDP",
        "nonfarm" => "PAYEMS",
        _ => return Err(format!("Unsupported indicator: {}", indicator).into()),
    };
    get_fred_series_value(pool, series_id).await?
        .ok_or_else(|| format!("No data for FRED series {}", series_id).into())
}

async fn fetch_url_content(url: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
    let client = Client::new();
    let resp = client.get(url).send().await?;
    let text = resp.text().await?;
    Ok(text)
}

// ========== 底层数据接口 ==========

async fn get_close_price(
    pool: &PgPool,
    symbol: &str,
    date: &NaiveDate,
) -> Result<Option<f64>, Box<dyn Error + Send + Sync>> {
    let cache_key = format!("yahoo:{}:{}", symbol, date);
    if let Some(cached) = get_cached_response(pool, &cache_key).await? {
        if let Some(price) = cached.as_f64() {
            return Ok(Some(price));
        }
    }

    let client = Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        .build()?;

    let start = date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();
    let end = date.succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();

    let url = format!(
        "https://query1.finance.yahoo.com/v8/finance/chart/{}?period1={}&period2={}&interval=1d",
        symbol, start, end
    );

    let resp = client.get(&url).send().await?;
    let json: Value = resp.json().await?;

    let price = json["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        .as_array()
        .and_then(|arr| arr.get(0).and_then(|v| v.as_f64()));

    if let Some(p) = price {
        let cache_value = serde_json::json!(p);
        set_cached_response(pool, &cache_key, &cache_value, 3600).await?;
        Ok(Some(p))
    } else {
        Ok(None)
    }
}

async fn get_fred_series_value(
    pool: &PgPool,
    series_id: &str,
) -> Result<Option<f64>, Box<dyn Error + Send + Sync>> {
    let cache_key = format!("fred:{}", series_id);
    if let Some(cached) = get_cached_response(pool, &cache_key).await? {
        if let Some(value) = cached["value"].as_f64() {
            return Ok(Some(value));
        }
    }

    let api_key = env::var("FRED_API_KEY")
        .map_err(|_| "FRED_API_KEY not set")?;
    let url = format!(
        "https://api.stlouisfed.org/fred/series/observations?series_id={}&api_key={}&file_type=json&sort_order=desc&limit=1",
        series_id, api_key
    );

    let client = Client::new();
    let resp = client.get(&url).send().await?;
    let json: Value = resp.json().await?;

    let value = json["observations"][0]["value"]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok());

    if let Some(v) = value {
        let cache_value = serde_json::json!({ "value": v });
        set_cached_response(pool, &cache_key, &cache_value, 3600).await?;
        Ok(Some(v))
    } else {
        Ok(None)
    }
}

#[allow(dead_code)]
fn bank_to_series_id(bank: &str) -> &str {
    match bank {
        "fomc" => "FEDFUNDS",
        "ecb" => "ECBASSETSW",
        "boj" => "JPNIRLTR",
        "pboc" => "CHNMLR",
        _ => "",
    }
}

// 全局缓存：使用 LazyLock 替代 lazy_static!
static CRUMB_CACHE: LazyLock<Arc<Mutex<Option<String>>>> = LazyLock::new(|| Arc::new(Mutex::new(None)));
static A3_COOKIE_CACHE: LazyLock<Arc<Mutex<Option<String>>>> = LazyLock::new(|| Arc::new(Mutex::new(None)));

// ========== 财报获取（修复 reqwest 方法） ==========
async fn fetch_earnings(
    pool: &PgPool,
    symbol: &str,
) -> Result<f64, Box<dyn Error + Send + Sync>> {
    let today = chrono::Utc::now().date_naive();
    let last_trading_day = get_last_trading_day(today);
    let start_date = last_trading_day;
    let end_date = start_date + Duration::days(1);

    let cache_key = format!("earnings:{}:{}", symbol, start_date);
    if let Some(cached) = get_cached_response(pool, &cache_key).await? {
        if let Some(eps) = cached["epsactual"].as_f64() {
            return Ok(eps);
        }
    }

    let crumb = get_crumb().await?;
    let a3_cookie = get_a3_cookie().await?;

    let client = Client::builder().build()?;

    // ---- 修改点1：手工拼接查询参数 ----
    let mut url = Url::parse("https://query2.finance.yahoo.com/v1/finance/visualization")?;
    url.query_pairs_mut()
        .clear()
        .extend_pairs([
            ("crumb", crumb.as_str()),
            ("lang", "en-US"),
            ("region", "US"),
            ("corsDomain", "finance.yahoo.com"),
        ]);

    let start_date_str = start_date.format("%Y-%m-%d").to_string();
    let end_date_str = end_date.format("%Y-%m-%d").to_string();

    let payload = serde_json::json!({
        "entityIdType": "earnings",
        "includeFields": [
            "ticker",
            "companyshortname",
            "eventname",
            "startdatetime",
            "startdatetimetype",
            "epsestimate",
            "epsactual",
            "epssurprisepct",
            "timeZoneShortName",
            "gmtOffsetMilliSeconds",
        ],
        "offset": 0,
        "query": {
            "operands": [
                { "operands": ["startdatetime", start_date_str], "operator": "gte" },
                { "operands": ["startdatetime", end_date_str], "operator": "lt" },
                { "operands": ["region", "us"], "operator": "eq" },
            ],
            "operator": "and",
        },
        "size": 100,
        "sortField": "companyshortname",
        "sortType": "ASC",
    });

    let response = client
        .post(url)                         // 直接使用构建好的 Url
        .header("Accept", "application/json, text/javascript, */*; q=0.01")
        .header("Content-Type", "application/json")
        .header("X-Requested-With", "XMLHttpRequest")
        .header("Referer", "https://finance.yahoo.com/calendar/earnings")
        .header("Cookie", format!("A3={}", a3_cookie))
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        .json(&payload)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(format!("Yahoo API 请求失败: {}", response.status()).into());
    }

    let json: Value = response.json().await?;
    let eps_actual = json["finance"]["result"][0]["documents"][0]["rows"]
        .as_array()
        .ok_or("Missing rows in response")?
        .iter()
        .find_map(|row| {
            let ticker = row[0].as_str().unwrap_or("");
            if ticker == symbol {
                row.get(5).and_then(|v| v.as_f64())
            } else {
                None
            }
        })
        .ok_or_else(|| format!("No EPS data found for symbol {}", symbol))?;

    let cache_value = serde_json::json!({ "epsactual": eps_actual });
    set_cached_response(pool, &cache_key, &cache_value, 3600).await?;
    Ok(eps_actual)
}

// ========== 工具函数 ==========
fn get_last_trading_day(date: NaiveDate) -> NaiveDate {
    let mut d = date;
    while d.weekday() == chrono::Weekday::Sat || d.weekday() == chrono::Weekday::Sun {
        d = d - Duration::days(1);
    }
    d
}

async fn get_crumb() -> Result<String, Box<dyn Error + Send + Sync>> {
    {
        let lock = CRUMB_CACHE.lock().await;
        if let Some(crumb) = lock.as_ref() {
            return Ok(crumb.clone());
        }
    }

    let client = Client::new();
    let resp = client
        .get("https://finance.yahoo.com")
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        .send()
        .await?;
    let body = resp.text().await?;

    let re = Regex::new(r#""crumb":"([^"]+)""#)?;
    let crumb = re
        .captures(&body)
        .and_then(|cap| cap.get(1).map(|m| m.as_str().to_string()))
        .ok_or("Failed to extract crumb from Yahoo Finance homepage")?;

    {
        let mut lock = CRUMB_CACHE.lock().await;
        *lock = Some(crumb.clone());
    }
    Ok(crumb)
}

async fn get_a3_cookie() -> Result<String, Box<dyn Error + Send + Sync>> {
    {
        let lock = A3_COOKIE_CACHE.lock().await;
        if let Some(cookie) = lock.as_ref() {
            return Ok(cookie.clone());
        }
    }

    if let Ok(cookie) = std::env::var("YAHOO_A3_COOKIE") {
        {
            let mut lock = A3_COOKIE_CACHE.lock().await;
            *lock = Some(cookie.clone());
        }
        return Ok(cookie);
    }

    let client = Client::new();
    let resp = client
        .get("https://finance.yahoo.com")
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        .send()
        .await?;

    // ---- 修改点2：手动解析 Set-Cookie 头 ----
    let headers = resp.headers();
    let set_cookie_headers = headers.get_all("set-cookie");
    for header in set_cookie_headers {
        if let Ok(cookie_str) = header.to_str() {
            // 解析 cookie 字符串，形如 "A3=value; path=/; domain=..."
            for part in cookie_str.split(';') {
                let part = part.trim();
                if let Some((key, value)) = part.split_once('=') {
                    if key.trim() == "A3" {
                        let a3 = value.trim().to_string();
                        // 缓存起来
                        {
                            let mut lock = A3_COOKIE_CACHE.lock().await;
                            *lock = Some(a3.clone());
                        }
                        return Ok(a3);
                    }
                }
            }
        }
    }

    Err("A3 cookie not found in response".into())
}