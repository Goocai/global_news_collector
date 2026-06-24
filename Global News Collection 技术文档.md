# 全球新闻阅读影响趋势预测系统

> **系统定位**：只负责采集和呈现原始新闻，由人类用户亲自阅读、摘取事实、做出概率推演，事后自动验证对比 LLM 的准确性。  
> **融合哲学**：反身性（索罗斯）、证伪主义（波普尔）、反脆弱（塔勒布）、行为金融学（塞勒）。  
> **开发环境**：Rust 1.96.0 stable | PostgreSQL（UTC 时区强制绑定）  

---

## 一、系统架构总览

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                          前端（浏览器）                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ 新闻列表页（标题、来源、时间、摘要，含“标记为噪音”按钮）              │   │
│  └─────────────────────────┬───────────────────────────────────────────┘   │
│                            ↓ 点击进入详情页                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ 新闻详情页 ———————— 阅读核心 ————————                               │   │
│  │ ┌─────────────────────────────────────────────────────────────────┐ │   │
│  │ │ 上半区：新闻全文（原始内容，无AI标注）                           │ │   │
│  │ └─────────────────────────────────────────────────────────────────┘ │   │
│  │ ┌─────────────────────────────────────────────────────────────────┐ │   │
│  │ │ 下半区：推演表单（默认折叠，点击展开）                            │ │   │
│  │ │   - 摘取事实（必填） \| 逻辑推演（必填）                           │ │   │
│  │ │   - 概率滑块（0-100） \| 虚拟仓位（强校验，0-100%）               │ │   │
│  │ │   - 预期兑现日（UTC日期）                                         │ │   │
│  │ │   - 规则类型选择（动态生成字段）                                 │ │   │
│  │ │   - 填写具体规则参数（如股票代码、阈值等）                       │ │   │
│  │ │   - 自己的预期参数（可选，可与系统规则不同）                     │ │   │
│  │ │   [提交预测]                                                     │ │   │
│  │ └─────────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ 统计面板页（反脆弱与认知行为纠偏）                                   │   │
│  │ - Brier曲线 + 不对称比卡片 \| 反脆弱气泡图 (Jitter 散点)             │   │
│  │ - 虚拟净值曲线（等权重 vs 仓位加权）                                 │   │
│  │ - 强制复盘列表（锁定高置信度错误 / 低概率命中）                       │   │
│  │ - 手动修正区（管理员专属，处理 failed_api 异常记录）                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │ HTTP/REST (JSON)
┌────────────────────────────────┴────────────────────────────────────────────┐
│                   后端（Rust + Axum）                                       │
│                                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │采集模块  │  │预测模块  │  │判定模块  │  │统计模块  │  │冷热分离  │   │
│  │- RSS轮询 │  │- 接收预测│  │- 定时扫表│  │- Brier   │  │定时任务  │   │
│  │- 过滤器  │  │- 任务入库│  │- 规则解析│  │- 不对称比│  │- 7天无   │   │
│  │- jieba-rs│  │- LLM熔断│  │- 状态机  │  │- 气泡图  │  │  预测清空│   │
│  │  动态词库│  │  工作流  │  │- 48h缓冲 │  │  数据    │  │  正文    │   │
│  └──────────┘  └──────────┘  └─────┬────┘  └──────────┘  └──────────┘   │
│                                    │                                       │
│  判定器：price_change | central_bank | url_keyword | economic_data         │
│  状态机：pending → judging → resolved / failed_api                         │
│                                                                             │
│  后台任务：采集调度器（动态热加载词库）                                      │
│           持久化 LLM 工作扫描流（FOR UPDATE SKIP LOCKED）                   │
│           自动化判定定时器 | 冷热分离常驻 Worker                            │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │ SQL (sqlx + PostgreSQL)
┌────────────────────────────────┴────────────────────────────────────────────┐
│                         PostgreSQL (含高度解耦的部分索引)                    │
│  sources | news | prediction_tasks | human_predictions | llm_predictions   │
│  users | noise_flags | brier_history | api_cache                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 二、核心模块职责

### 2.1 采集模块与动态噪声闭环

- **调度器**：启动时从 `sources` 表读取所有 `enabled=true` 的源，为每个源创建独立的 Tokio 定时任务，使用该源的 `refresh_interval_sec` 作为间隔。每隔 60 秒重新读取源列表，动态增删任务或调整间隔（热加载）。
- **去重与存储**：依据 `url` 唯一性去重；若无有效 URL，则用 `标题+发布时间` 的 SHA256 作为备用哈希。原始数据存入 `news` 表，`content` 字段保留全文（供用户阅读）。
- **前置关键词过滤与噪声动态反馈闭环**：
  - 静态过滤：必须包含 `require_keywords` 且不含 `exclude_keywords` 才能入库。
  - 动态优化闭环（集成 `jieba-rs` 中文分词）：内存中维护 `Arc<RwLock<HashSet<String>>>` 动态排除词库。每小时扫描 `noise_flags` 表，统计近 7 天内被独立用户标记超过 3 次的新闻标题，分词后抽取高频词，经管理员审阅或自动注入，追加至排除词库，实现采集器自适应进化。

---

### 2.2 预测模块

#### 2.2.1 数据模型总览

- **`prediction_tasks`**：存储预测任务的公共信息，包括 **系统判定规则（`rule_json`）**，该规则包含所有具体参数（如 `symbol`、`threshold_percent` 等），由判定器直接解析执行。
- **`human_predictions`** 和 **`llm_predictions`**：各自存储 **推理文本（`inference`）**、**概率（`probability`）**、**虚拟仓位（仅人类）** 以及 **各自的主观预期参数（`inference_rule`）**。`inference_rule` 的结构与 `rule_json` 相同，但具体数值可能不同，用于事后对比。

#### 2.2.2 规则类型元数据（全局）

系统预定义规则类型及其所需字段，这些信息存储在代码配置中，并通过 API 提供给前端。例如：

```json
{
  "price_change": {
    "fields": [
      {"name": "symbol", "type": "string", "required": true, "label": "股票代码", "placeholder": "AAPL"},
      {"name": "market", "type": "string", "required": true, "label": "市场", "options": ["us","hk","cn"]},
      {"name": "base_date", "type": "date", "required": true, "label": "基准日期"},
      {"name": "observation_date", "type": "date", "required": true, "label": "观察日期"},
      {"name": "threshold_percent", "type": "number", "required": true, "label": "阈值百分比", "min": 0, "max": 100},
      {"name": "direction", "type": "string", "required": true, "label": "方向", "options": ["greater","less"]},
      {"name": "target_date", "type": "date", "required": true, "label": "截止日期"}
    ]
  },
  "central_bank": { ... },
  "url_keyword": { ... },
  "economic_data": { ... }
}
```

前端通过 `GET /api/rule-types` 获取此元数据，动态渲染表单。用户填写所有必填字段后，提交的 `rule_json` 为完整的键值对对象，例如：

```json
{
  "type": "price_change",
  "symbol": "AAPL",
  "market": "us",
  "base_date": "2026-05-23",
  "observation_date": "2026-06-06",
  "threshold_percent": 5.0,
  "direction": "greater",
  "target_date": "2026-06-06"
}
```

此外，用户可额外填写自己的预期参数（`inference_rule`），其结构完全一致，但数值可调整（如阈值改为 3.0）。若未填写，则默认与 `rule_json` 相同。

#### 2.2.3 预测提交流程

- **API**：`POST /api/predictions` 接收：
  ```json
  {
    "news_id": 123,
    "extracted_facts": "美联储宣布加息25基点",
    "inference": "市场已充分预期，预计美股上涨",
    "probability": 70.0,
    "position_size_pct": 80.0,
    "rule_json": { "type": "price_change", "symbol": "AAPL", ... },
    "inference_rule": { "type": "price_change", "symbol": "AAPL", "threshold_percent": 3.0, ... }, // 可选
    "target_date": "2026-06-06"
  }
  ```
- **后端处理**：
  1. 验证 `rule_json` 是否符合对应类型的字段要求。
  2. 插入 `prediction_tasks`（`rule_json` 存储系统规则，`outcome_human/outcome_llm` 为 NULL）。
  3. 插入 `human_predictions`（`inference_rule` 存储人类预期，若未提供则复制 `rule_json`）。
  4. 返回 `task_id`，异步触发 LLM 工作流。

#### 2.2.4 LLM 对照组生成与熔断

- **持久化队列**：Worker 使用 `FOR UPDATE SKIP LOCKED` 扫描 `llm_predicted=false` 的任务。
- **熔断条件**（满足任一则跳过）：
  - 新闻被标记为噪音（`noise_flags` 存在记录）。
  - 新闻标题+摘要总字数 < 20。
  - 新闻发布时间距今超过 3 天。
- **LLM 调用**：将新闻标题、摘要、`extracted_facts` 以及系统规则（`rule_json`）传入 LLM，要求其返回 `inference`、`probability` 及 `inference_rule`（LLM 的主观预期参数）。
- **存储**：成功后插入 `llm_predictions`，并设置 `llm_predicted=true`；若熔断或失败，同样将 `llm_predicted` 置为 true（跳过）。

---

### 2.3 判定模块

- **定时任务**：每小时扫描 `prediction_tasks`，筛选 `outcome_human IS NULL OR outcome_llm IS NULL` 且 `judge_status IN ('pending','failed_api')` 的任务。
- **判定流程**：
  1. 解析 `rule_json`，根据 `type` 调用对应判定器（如 `price_change` 调用 Yahoo Finance API）。
  2. 若成功获取客观结果（0 或 1），在同一事务中更新 `outcome_human` 和 `outcome_llm` 为相同值，状态改为 `resolved`，记录 `verified_at`。
  3. 若 API 失败，状态改为 `failed_api`，进入 48 小时缓冲期。
- **到期判负**：对于 `target_date < CURRENT_DATE` 且仍未判定的任务，强制将 `outcome_human` 和 `outcome_llm` 置为 0，状态 `resolved`。对于 `failed_api` 状态，缓冲期过后同样判负。
- **管理员干预**：通过 `POST /api/admin/predictions/{id}/override` 可单独修正 `outcome_human` 或 `outcome_llm`，用于特殊场景（如人类获得额外信息，而 LLM 保持自动判定）。

---

### 2.4 统计模块

- **Brier Score**：分别基于 `human_predictions.probability` 与 `outcome_human`，以及 `llm_predictions.probability` 与 `outcome_llm` 计算。每日快照存入 `brier_history`。
- **不对称比**：仅针对人类数据（`position_size_pct` 与 `outcome_human`），统计置信度 ≥50% 的记录，计算 `正确时平均仓位 / 错误时平均仓位`，目标 > 1.5。
- **强制复盘**：自动捕获 `probability >= 70 && outcome_human = 0` 或 `probability <= 30 && outcome_human = 1` 的任务，要求用户填写 `post_mortem`，否则红标警告。
- **气泡图**：X轴为人类概率，Y轴为结果（含 Jitter），气泡大小代表仓位，突出显示错误且重仓的点。

---

### 2.5 Web 模块

- RESTful API（详见附录）。
- 静态文件服务：嵌入前端（HTML + Tailwind + Alpine.js + Chart.js）。
- JWT 认证中间件。

### 2.6 冷热分离

- 每天凌晨 3:00 清理 `published_at < NOW() - INTERVAL '7 days'` 且无关联 `prediction_tasks` 的新闻，将其 `content` 置为 NULL。

---

## 三、数据库设计（最终版）

```sql
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

```

---

## 四、前端设计

- **技术栈**：HTML5 + Tailwind CSS v3 (CDN) + Alpine.js v3 + Chart.js。
- **核心交互**：
  - **新闻列表**：卡片展示，含“标记为噪音”按钮。
  - **详情页**：上部原文（若 content 为 NULL 则提示归档），下部推演表单。
  - **推演表单**：
    - `extracted_facts` 必填。
    - 规则类型下拉框，选择后动态显示对应字段（通过 `GET /api/rule-types` 获取元数据）。
    - 系统规则参数填写完整后，可展开“高级预期”，填写自己的预期参数（可选）。
    - 仓位滑块无默认值，必须手动拖动。
    - 目标日期最小可选为服务器 UTC 日期 + 1 天（从 `/api/time` 获取）。
  - **统计面板**：不对称比卡片、气泡图、净值曲线、待复盘列表（红标），点击弹窗提交复盘。

---

## 五、判定规则示例与元数据定义

系统内置的规则类型及其字段定义如下（存储在代码中，通过 API 暴露）：

```json
{
  "price_change": {
    "label": "价格涨跌幅",
    "fields": [
      { "name": "symbol", "type": "string", "required": true, "label": "股票代码" },
      { "name": "market", "type": "string", "required": true, "label": "市场", "options": ["us","hk","cn"] },
      { "name": "base_date", "type": "date", "required": true, "label": "基准日期" },
      { "name": "observation_date", "type": "date", "required": true, "label": "观察日期" },
      { "name": "threshold_percent", "type": "number", "required": true, "label": "阈值百分比", "min": 0 },
      { "name": "direction", "type": "string", "required": true, "label": "方向", "options": ["greater","less"] }
    ]
  },
  "central_bank": {
    "label": "央行利率决议",
    "fields": [
      { "name": "bank", "type": "string", "required": true, "label": "央行", "options": ["fomc","ecb","boj","pboc"] },
      { "name": "meeting_date", "type": "date", "required": true, "label": "会议日期" },
      { "name": "expected_action", "type": "string", "required": true, "label": "预期行动", "options": ["hike_25bp","hold","cut_25bp"] }
    ]
  },
  "url_keyword": {
    "label": "URL关键词检测",
    "fields": [
      { "name": "url", "type": "string", "required": true, "label": "目标 URL" },
      { "name": "keywords", "type": "array", "required": true, "label": "关键词列表" },
      { "name": "match_all", "type": "boolean", "required": true, "label": "全部匹配" }
    ]
  },
  "economic_data": {
    "label": "经济数据对比",
    "fields": [
      { "name": "indicator", "type": "string", "required": true, "label": "指标", "options": ["cpi","gdp","nonfarm"] },
      { "name": "country", "type": "string", "required": true, "label": "国家" },
      { "name": "release_date", "type": "date", "required": true, "label": "发布日期" },
      { "name": "expected_value", "type": "number", "required": true, "label": "预期值" },
      { "name": "operator", "type": "string", "required": true, "label": "比较符", "options": ["greater","less","equal"] },
      { "name": "actual_value_source", "type": "string", "required": true, "label": "实际值来源", "options": ["manual","api"] },
      { "name": "actual_value", "type": "number", "required": false, "label": "实际值（手动）", "depends_on": { "field": "actual_value_source", "value": "manual" } }
    ]
  },
  "earnings": {
    "label": "财报业绩",
    "fields": [
      { "name": "symbol", "type": "string", "required": true, "label": "股票代码" },
      { "name": "market", "type": "string", "required": true, "label": "市场", "options": ["us","hk","cn"] },
      { "name": "quarter", "type": "string", "required": true, "label": "季度", "placeholder": "2026Q2" },
      { "name": "expected_eps", "type": "number", "required": true, "label": "预期每股收益" },
      { "name": "actual_eps_source", "type": "string", "required": true, "label": "实际值来源", "options": ["manual","api"] },
      { "name": "actual_eps", "type": "number", "required": false, "label": "实际每股收益（手动）", "depends_on": { "field": "actual_eps_source", "value": "manual" } }
    ]
  },
  "commodity": {
    "label": "大宗商品价格",
    "fields": [
      { "name": "symbol", "type": "string", "required": true, "label": "商品代码", "placeholder": "CL=F" },
      { "name": "market", "type": "string", "required": true, "label": "市场", "options": ["us","hk","cn"] },
      { "name": "base_date", "type": "date", "required": true, "label": "基准日期" },
      { "name": "observation_date", "type": "date", "required": true, "label": "观察日期" },
      { "name": "threshold_percent", "type": "number", "required": true, "label": "阈值百分比", "min": 0 },
      { "name": "direction", "type": "string", "required": true, "label": "方向", "options": ["greater","less"] }
    ]
  }
}
```
---

## 六、配置文件 (`config.toml`)

```toml
[server]
host = "127.0.0.1"
port = 8080
timezone = "UTC"

[collector]
default_refresh_interval_sec = 300
user_agent = "GlobalNewsMonitor/1.0"
request_timeout_sec = 30
retry_count = 2

[collector.filter]
require_keywords = ["加息", "CPI", "非农", "利率决议", "财报", "回购", "减持", "地缘", "制策"]
exclude_keywords = ["专家表示", "或将", "可能", "建议投资者", "疑似", "传闻"]

[llm]
provider = "gemini"
model = "gemini-1.5-flash"
temperature = 0.2
max_tokens = 200
timeout_sec = 30

[verifier]
judge_interval_sec = 3600
data_fetch_retry = 2
failed_api_grace_period_hours = 48
yahoo_finance_base_url = "https://query1.finance.yahoo.com/v8/finance/chart/"

[storage]
hot_days = 7
cleanup_cron = "0 3 * * *"

[auth]
jwt_secret = "change_me_in_production"
token_expire_hours = 24
```

---

## 七、完整开发路线图

| 步骤 | 模块 | 任务 |
|------|------|------|
| 1 | 项目骨架 | 初始化 Rust 项目，添加依赖 |
| 2 | 数据库迁移 | 执行上述 DDL |
| 3 | 采集器 | RSS 解析、轮询、去重入库 |
| 4 | 新闻 API | `GET /api/news` 和 `/api/news/{id}` |
| 5 | 前端基础 | 列表页、噪音按钮 |
| 6 | 前端详情 | 展示原文，推演表单（动态规则字段） |
| 7 | 预测提交 API | 接收完整 rule_json 和 inference_rule，插入任务及人类详情 |
| 8 | 判定模块 | 状态机、到期判负、API 调用 |
| 9 | LLM Worker | 扫描 llm_predicted=false，熔断，调用 LLM，存储 llm_predictions |
| 10 | 统计基础 | Brier 计算与历史快照 |
| 11 | 反脆弱指标 | 不对称比、净值曲线、气泡图 |
| 12 | 冷热分离 | 凌晨清理无任务新闻 content |
| 13 | 认知纠偏前端 | 气泡图、不对称比卡片、强制复盘 |
| 14 | 判定器实现 | 具体对接 Yahoo Finance、央行数据等 |
| 15 | 用户认证 | JWT 注册/登录，中间件 |
| 16 | 管理员功能 | 手动 override outcome，噪声词审核 |
| 17 | 测试与部署 | 单元测试，API 文档，Docker 化 |

---

## 八、核心理念与技术落地映射表

| 理念 | 落地 |
|------|------|
| 反身性 | 盲测异步 LLM；持久化队列防止任务丢失 |
| 行为金融学 | 噪音标记 → 动态词库；强制复盘对抗确认偏误 |
| 证伪主义 | 到期自动判负；全局 UTC |
| 反脆弱 | 仓位强校验；不对称比；气泡图；48h 缓冲 |
| 冷热分离 | 7 天无预测新闻 content 置 NULL |

---

## 九、主要 API 端点

| 方法 | 路径 | 权限 | 说明 |
|------|------|------|------|
| GET | `/api/news` | 登录 | 分页新闻列表 |
| GET | `/api/news/{id}` | 登录 | 详情（若 content 为 NULL 则提示归档） |
| POST | `/api/news/{id}/noise` | 专家 | 标记噪音 |
| POST | `/api/predictions` | 专家 | 提交预测（含 rule_json 和可选 inference_rule） |
| POST | `/api/predictions/{id}/post_mortem` | 专家 | 提交复盘 |
| GET | `/api/stats/anti-fragile` | 专家 | 不对称比、气泡图、净值曲线数据 |
| GET | `/api/stats/brier` | 专家 | Brier 历史曲线 |
| POST | `/api/admin/predictions/{id}/override` | 管理员 | 修正 outcome_human 或 outcome_llm |
| GET | `/api/rule-types` | 登录 | 获取所有规则类型及其字段定义（用于前端动态表单） |
| GET | `/api/time` | 公开 | 服务器当前 UTC 日期 |
| POST | `/api/auth/login` | 公开 | 登录 |
| POST | `/api/auth/register` | 公开 | 注册 |

---

## 十、附录：改进清单与设计决策

- **数据模型**：拆分为任务表 + 人类/LLM 详情表，各自存储规则（系统规则与个人预期分开）。
- **规则元数据**：集中管理，前端动态渲染，避免硬编码。
- **`rule_json`**：存储系统判定所需的完整参数，由判定器直接执行。
- **`inference_rule`**：存储个人预期，用于事后对比，不参与判定。
- **`outcome_human` 与 `outcome_llm`**：分别存储，允许管理员独立修正。
- **`extracted_facts` 必填**：确保用户基于事实下注。
- **熔断条件**：噪声、信息量（标题+摘要长度）、时效性。

---

> **本方案为系统最终设计蓝本，涵盖从需求到部署的全部细节。**