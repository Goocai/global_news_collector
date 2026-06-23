-- 迁移文件：initial_schema
-- 强制使用 UTC 时区（PostgreSQL 默认，显式声明无妨）
SET TIME ZONE 'UTC';

-- 用户表
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL CHECK (role IN ('human_expert', 'admin')),
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT (now() AT TIME ZONE 'UTC')
);

-- 采集源表
CREATE TABLE sources (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    feed_type TEXT NOT NULL CHECK (feed_type IN ('rss', 'atom', 'api')),
    refresh_interval_sec INT DEFAULT 300,
    enabled BOOLEAN DEFAULT true,
    require_keywords TEXT[],
    exclude_keywords TEXT[]
);

-- 新闻表
CREATE TABLE news (
    id SERIAL PRIMARY KEY,
    source_id INT REFERENCES sources(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    description TEXT,          -- 新增：RSS 摘要/描述
    content TEXT,
    content_fetching BOOLEAN DEFAULT false,
    url TEXT UNIQUE NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    fetched_at TIMESTAMPTZ DEFAULT (now() AT TIME ZONE 'UTC')

);

-- 预测表（核心）
CREATE TABLE predictions (
    id SERIAL PRIMARY KEY,
    news_id INT REFERENCES news(id) ON DELETE CASCADE,
    user_id INT REFERENCES users(id) ON DELETE SET NULL,
    prediction_type TEXT NOT NULL CHECK (prediction_type IN ('human', 'llm')),
    extracted_facts TEXT,
    inference TEXT NOT NULL,
    probability DECIMAL(5,2) NOT NULL CHECK (probability BETWEEN 0.00 AND 100.00),
    position_size_pct DECIMAL(5,2) NOT NULL CHECK (position_size_pct BETWEEN 0.00 AND 100.00),
    rule_json JSONB,
    outcome INT CHECK (outcome IN (0, 1)),
    parent_prediction_id INT REFERENCES predictions(id) ON DELETE CASCADE,
    target_date DATE NOT NULL,
    judge_status TEXT DEFAULT 'pending' CHECK (judge_status IN ('pending', 'judging', 'resolved', 'failed_api')),
    post_mortem TEXT,
    llm_skip BOOLEAN DEFAULT false,
    submitted_at TIMESTAMPTZ DEFAULT (now() AT TIME ZONE 'UTC'),
    verified_at TIMESTAMPTZ
);

-- 噪声标记表（用户反馈）
CREATE TABLE noise_flags (
    id SERIAL PRIMARY KEY,
    news_id INT NOT NULL REFERENCES news(id) ON DELETE CASCADE,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    flagged_at TIMESTAMPTZ DEFAULT (now() AT TIME ZONE 'UTC'),
    CONSTRAINT unique_news_user UNIQUE (news_id, user_id)
);

-- Brier 历史快照表
CREATE TABLE brier_history (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id) ON DELETE CASCADE,
    calculation_time TIMESTAMPTZ DEFAULT (now() AT TIME ZONE 'UTC'),
    human_brier DECIMAL(10,4) NOT NULL,
    llm_brier DECIMAL(10,4) NOT NULL,
    asymmetry_ratio DECIMAL(10,4) NOT NULL,
    delta DECIMAL(10,4) GENERATED ALWAYS AS (human_brier - llm_brier) STORED
);


-- Add migration script here
-- 创建 API 缓存表
CREATE TABLE api_cache (
    id SERIAL PRIMARY KEY,
    cache_key TEXT NOT NULL UNIQUE,          -- 请求的唯一标识
    response_data JSONB NOT NULL,            -- 存储完整的响应 JSON
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL          -- 过期时间
);


-- 索引（用于查询性能，后续再根据需要添加）
CREATE INDEX idx_news_published_at ON news(published_at DESC);
CREATE UNIQUE INDEX idx_news_url_unique ON news(url);
CREATE INDEX idx_predictions_active_judge ON predictions(judge_status, target_date) WHERE outcome IS NULL;
CREATE INDEX idx_predictions_llm_queue_scan ON predictions(prediction_type, id) WHERE llm_skip = false;
CREATE INDEX idx_predictions_parent_link ON predictions(parent_prediction_id) WHERE parent_prediction_id IS NOT NULL;
CREATE INDEX idx_predictions_news_type ON predictions(news_id, prediction_type);
CREATE INDEX idx_noise_flags_lookup ON noise_flags(news_id, flagged_at);

-- 索引加速查询和清理
CREATE INDEX idx_api_cache_key ON api_cache(cache_key);
CREATE INDEX idx_api_cache_expires_at ON api_cache(expires_at);

-- 示例：添加新华社国际频道
INSERT INTO sources (name, url, feed_type, refresh_interval_sec, enabled) 
VALUES ('中新网财经新闻', 'https://www.chinanews.com.cn/rss/finance.xml', 'rss', 600, true);


