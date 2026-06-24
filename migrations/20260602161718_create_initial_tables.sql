-- 强制使用 UTC 时区
SET TIME ZONE 'UTC';

-- 用户表
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL CHECK (role IN ('human', 'admin')),
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
    description TEXT,
    content TEXT,
    url TEXT UNIQUE NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    fetched_at TIMESTAMPTZ DEFAULT (now() AT TIME ZONE 'UTC')
);

-- 预测任务主表
CREATE TABLE prediction_tasks (
    id SERIAL PRIMARY KEY,
    news_id INT REFERENCES news(id) ON DELETE CASCADE,
    user_id INT REFERENCES users(id) ON DELETE SET NULL,
    extracted_facts TEXT NOT NULL,          
    rule_json JSONB NOT NULL,                
    outcome_human INT CHECK (outcome_human IN (0, 1)),
    outcome_llm   INT CHECK (outcome_llm   IN (0, 1)),
    target_date DATE NOT NULL,
    judge_status TEXT DEFAULT 'pending' CHECK (judge_status IN ('pending', 'judging', 'resolved', 'failed_api')),
    post_mortem TEXT,
    llm_predicted BOOLEAN DEFAULT false,         -- true：已尝试生成 LLM 预测（或已跳过）
    submitted_at TIMESTAMPTZ DEFAULT (now() AT TIME ZONE 'UTC'),
    verified_at TIMESTAMPTZ
);

-- 人类预测详情（含 inference_rule）
CREATE TABLE human_predictions (
    id SERIAL PRIMARY KEY,
    task_id INT REFERENCES prediction_tasks(id) ON DELETE CASCADE,
    inference TEXT NOT NULL,                     -- 推理逻辑说明
    inference_rule JSONB NOT NULL,                        -- 人类自己的预期参数
    probability DECIMAL(5,2) NOT NULL CHECK (probability BETWEEN 0 AND 100),
    position_size_pct DECIMAL(5,2) NOT NULL CHECK (position_size_pct BETWEEN 0 AND 100)
);

-- LLM 预测详情（含 inference_rule）
CREATE TABLE llm_predictions (
    id SERIAL PRIMARY KEY,
    task_id INT REFERENCES prediction_tasks(id) ON DELETE CASCADE,
    inference TEXT NOT NULL,
    inference_rule JSONB NOT NULL,                        -- LLM 自己的预期参数
    probability DECIMAL(5,2) NOT NULL CHECK (probability BETWEEN 0 AND 100),
    position_size_pct DECIMAL(5,2) NOT NULL CHECK (position_size_pct BETWEEN 0 AND 100)
);

-- 噪声标记表
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

-- API 缓存表
CREATE TABLE api_cache (
    id SERIAL PRIMARY KEY,
    cache_key TEXT NOT NULL UNIQUE,
    response_data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);

-- ========== 索引 ==========
CREATE INDEX idx_news_published_at ON news(published_at DESC);
CREATE UNIQUE INDEX idx_news_url_unique ON news(url);

-- 判定扫描：未决且状态为 pending/failed_api 的任务
CREATE INDEX idx_tasks_judge ON prediction_tasks(judge_status, target_date)
    WHERE outcome_human IS NULL OR outcome_llm IS NULL;

-- LLM 队列扫描：未进行 LLM 预测的任务
CREATE INDEX idx_tasks_llm_queue ON prediction_tasks(id)
    WHERE llm_predicted = false;

-- 关联查询索引
CREATE INDEX idx_tasks_news ON prediction_tasks(news_id);
CREATE INDEX idx_human_task ON human_predictions(task_id);
CREATE INDEX idx_llm_task ON llm_predictions(task_id);
CREATE INDEX idx_noise_flags_lookup ON noise_flags(news_id, flagged_at);
CREATE INDEX idx_api_cache_expires ON api_cache(expires_at);

-- 加速按用户筛选 + 排序
CREATE INDEX idx_tasks_user_submitted ON prediction_tasks(user_id, submitted_at DESC);

-- 加速结果已确定且无事后分析的查询
CREATE INDEX idx_tasks_resolved_no_postmortem ON prediction_tasks(user_id, submitted_at DESC)
    WHERE outcome_human IS NOT NULL AND outcome_llm IS NOT NULL
      AND (post_mortem IS NULL OR post_mortem = '');

-- 示例数据
INSERT INTO sources (name, url, feed_type, refresh_interval_sec, enabled)
VALUES ('中新网财经新闻', 'https://www.chinanews.com.cn/rss/finance.xml', 'rss', 600, true);