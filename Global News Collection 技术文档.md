# 全球新闻阅读影响趋势预测系统技术方案 

> **系统只负责采集和呈现原始新闻，由人类用户亲自阅读、提取事实、做出概率推演，事后自动验证对比 LLM 的准确性。**
> **融合反身性、证伪主义、反脆弱理念，并针对信噪比、反脆弱可视化、认知闭环与系统运维进行了增强。**
> 开发环境：Rust 1.96.0 stable | 数据库：PostgreSQL (强绑定 UTC 时区)

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
│  │ │   - 摘取事实（可选） \| 推演描述（必填）                           │ │   │
│  │ │   - 概率滑块（0-100） \| 虚拟仓位比例（强校验，必填，0-100%）        │ │   │
│  │ │   - 预期兑现日（强锁定 UTC 日期） \| 判定规则（模板+参数）          │ │   │
│  │ │   [提交预测]                                                     │ │   │
│  │ └─────────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ 统计面板页（反脆弱与认知行为纠偏）                                   │   │
│  │ - Brier曲线 + 不对称比卡片 \| 反脆弱气泡图 (Jitter 散点)             │   │
│  │ - 虚拟净值曲线（等权重 vs 仓位加权）                                 │   │
│  │ - 强制复盘列表（锁定高置信度错误 / 低概率命中）                       │   │
│  │ - 手动修正区（管理员专属，处理挂起的 failed_api 异常记录）           │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │ HTTP/REST (JSON)
┌────────────────────────────────┴────────────────────────────────────────────┐
│                   后端（Rust + Axum）                                       │
│                                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │采集模块  │  │预测模块  │  │判定模块  │  │统计模块  │  │冷热分离  │   │
│  │- RSS轮询 │  │- 接收预测│  │- 定时扫表│  │- Brier   │  │定时任务  │   │
│  │- 过滤器  │  │- DB状态流│  │- 规则解析│  │- 不对称比│  │- 7天无预测│   │
│  │- jieba-rs│  │  无损队列│  │- 状态机  │  │- 气泡图  │  │  正文清空│   │
│  │  动态词库│  │- 熔断控制│  │- 48h缓冲 │  │  数据    │  │          │   │
│  └──────────┘  └──────────┘  └─────┬────┘  └──────────┘  └──────────┘   │
│                                    │                                       │
│  判定器：price_change | central_bank | url_keyword | economic_data         │
│  状态机：pending → judging → resolved / failed_api                         │
│                                                                             │
│  后台任务：采集调度器（动态热加载词库）                                      │
│           持久化事务型 LLM 工作扫描流 | 自动化判定定时器 | 冷热分离常驻 Worker  │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │ SQL (sqlx + PostgreSQL)
┌────────────────────────────────┴────────────────────────────────────────────┐
│                         PostgreSQL (内含高度解耦的部分索引)                  │
│  sources | news | predictions | users | brier_history | noise_flags       │
└─────────────────────────────────────────────────────────────────────────────┘

```

---

## 二、核心模块职责

### 2.1 采集模块与动态噪声闭环

* **调度器**：启动时从 `sources` 表读取所有 `enabled=true` 的源，为每个源创建独立的 Tokio 定时任务，使用该源的 `refresh_interval_sec` 作为间隔。每隔 60 秒重新读取源列表，动态增删任务或调整间隔（热加载）。
* **去重与存储**：依据 `url` 唯一性去重；若无有效 URL，则用 `标题+发布时间` 的 SHA256 作为备用哈希。原始数据存入 `news` 表，`content` 字段保留全文。
* **前置关键词过滤与噪声动态反馈闭环**：
* 采集系统内置静态过滤逻辑：必须包含 `require_keywords` 且不含 `exclude_keywords` 才能入库。
* **动态优化闭环（集成 `jieba-rs` 中文分词）**：为打通人类反馈到过滤器的闭环，采集模块在内存中维护一个 `Arc<RwLock<HashSet<String>>>` 的动态排除词库。系统每小时异步扫描 `noise_flags` 表并统计近 7 天内被独立用户标记为噪音超过 3 次的新闻标题。为了解决国标/中文环境下缺乏天然空格分词的问题，系统引入 **`jieba-rs`（结巴分词 Rust 版）** 对标题进行深度分词，自动过滤掉内置的停用词（如“的”、“了”、“关于”），统计噪声高频词组并建立 `HashMap` 进行词频（TF）排序。Top 词汇经由管理员审阅或自动注入，动态追加至排除词库中，实现采集器的自适应噪声进化，无需重启服务。



### 2.2 预测模块与 LLM 熔断控制

* **HTTP API**：`POST /api/predictions` 接收前端提交的预测数据。
* **人类预测存储**：插入 `predictions` 表，`prediction_type='human'`。`position_size_pct`（虚拟仓位）与 `target_date`（预期兑现日）在业务层与数据库层均设为强校验必填。
* **持久化状态任务流（消除内存丢失隐患）**：
* 为彻底消除纯内存 `mpsc` 通道在服务器突发故障、重启或版本部署时导致的任务丢失隐患（保障人类与 LLM 盲测对抗记录的绝对对齐），系统改用**基于数据库状态流转的持久化任务流**。
* 后台常驻的 LLM Worker 线程不再监听内存通道，而是采用高性能分批捞取机制，利用 PostgreSQL 的 **`FOR UPDATE SKIP LOCKED`** 实施并发无锁安全扫描。Worker 每隔 5 秒捞取未触发熔断且尚无 LLM 对照组的记录：
```sql
SELECT id, news_id FROM predictions p
WHERE p.prediction_type = 'human' AND p.llm_skip = false
  AND NOT EXISTS (SELECT 1 FROM predictions l WHERE l.parent_prediction_id = p.id)
ORDER BY p.id ASC LIMIT 10 FOR UPDATE SKIP LOCKED;

```


* 捞取记录后，Worker 优先对关联的新闻执行 **熔断检查流水线**：
1. **噪声熔断**：检查当前新闻在 `noise_flags` 中是否已被用户标记。
2. **信息量熔断**：检查 `news.content` 文本长度是否小于 100 字。
3. **时效性熔断**：检查新闻发布时间是否已超过 3 天（`NOW() - published_at > INTERVAL '3 days'`）。


* 若触发任一熔断条件，系统直接将人类预测的 `llm_skip` 字段更新为 `true`，终止后续 API 请求。若未熔断，则调用 LLM API，若大模型返回 `{"skip": true}`（主动弃权），同样更新 `llm_skip = true`；若正常生成，则插入 `prediction_type='llm'` 的对照记录，并通过 `parent_prediction_id` 指向人类预测。



### 2.3 判定模块与状态机

* **定时任务**：每隔 1 小时扫描 `predictions` 表中 `outcome IS NULL` 且状态为待判定（`pending` 或 `failed_api`）的人类预测。
* **判定状态机流转控制**：
* `pending`：初始状态，等待到达 `target_date` 或规则触发。
* `judging`：正在由自动化判定器处理中，避免多 Worker 重复执行。
* `resolved`：判定成功，已写入 outcome。
* `failed_api`：因网络请求或第三方 API 异常，进入挂起重试态。


* **状态机自动终止（到期判负）流转逻辑**：
* 证伪主义核心在于“过时未证实即为伪”。判定模块在每次执行时，会优先通过 **PostgreSQL 事务** 处理所有已越过 `target_date` 且外部规则未被触发的预测，强制使其自动终止流转。
* **工程时区防御与异常公平性修正**：系统全局（包括前端、后端、PostgreSQL 数据库）强行锁定采用 **UTC 时区**。判定条件统一采用 `(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date`，彻底杜绝服务器时区交叉带来的“过早判负”乌龙。同时，为防止因外部第三方接口临时挂掉导致数据进入 `failed_api` 状态、而在到期日当天被粗暴“误杀判负”从而污染人类专家的 Brier 分数，事务设计中对 `failed_api` 状态引入了 **48小时异常重试缓冲期**。只有超出缓冲期仍未恢复的故障预测，才允许触发自动归零判负。
* **同步判负事务 SQL**：
```sql
BEGIN;
-- 1. 将到期未决的人类预测直接判定为证伪(0)，状态转为 resolved。对因网络故障导致的 failed_api，给予 2 天的缓冲宽限期
UPDATE predictions
SET judge_status = 'resolved', outcome = 0, verified_at = NOW() AT TIME ZONE 'UTC'
WHERE outcome IS NULL 
  AND prediction_type = 'human' 
  AND (
    (judge_status = 'pending' AND target_date < (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date)
    OR 
    (judge_status = 'failed_api' AND target_date < ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - INTERVAL '2 days')::date)
  );

-- 2. 通过关联字段，将对应的 LLM 对照预测同步判负(0)，状态转为 resolved
UPDATE predictions llm
SET judge_status = 'resolved', outcome = 0, verified_at = NOW() AT TIME ZONE 'UTC'
FROM predictions human
WHERE llm.parent_prediction_id = human.id 
  AND llm.prediction_type = 'llm' 
  AND llm.outcome IS NULL
  AND human.judge_status = 'resolved' 
  AND human.outcome = 0;
COMMIT;

```





### 2.4 统计模块（反脆弱与认知纠偏）

* **Brier Score 计算**：公式：`BS = (1/N) * Σ (p/100 - o)²`。分别计算人类与 LLM 的得分，每日定时将快照存入 `brier_history` 供前端绘制对抗曲线。
* **反脆弱核心指标——不对称比 (Asymmetry Ratio)**：
* 公式：`ASYM = (正确预测时的平均仓位) / (错误预测时的平均仓位)`。
* 统计范围：仅针对置信度大于等于 50% 的确定性预测（人类）。系统目标值大于 1.5，用于量化人类是否做到了“确信时重仓下注，不确定时轻仓试错”。


* **行为金融学控制——强置复盘纠偏机制**：
* 系统根据统计数据，自动将符合以下两类认知偏差特征的预测推入 **“待复盘黑名单”**：
1. **过度自信（高置信度错误）**：`probability >= 70` 且 `outcome = 0`。
2. **幸存者偏差（低概率命中）**：`probability <= 30` 且 `outcome = 1`。


* **业务强约束**：属于上述两类的预测，系统将强制锁定其状态，用户必须在前端提交 `post_mortem`（反思复盘文本）后，方可解除统计面板的警示红标，用制度对抗“确认偏误”。



### 2.5 Web 模块

* **路由分配**：提供标准 RESTful API 满足新闻获取、人类预测提交、反脆弱指标拉取（`asymmetry`, `bubble`, `equity_curve`）、噪音标记、强制复盘提交等。
* **静态文件服务**：使用 `tower_http::services::ServeDir` 将单页前端嵌入 Axum 服务。

### 2.6 冷热数据分离（隔离归档）

* **常驻后台 Worker**：在系统启动时通过 `tokio::spawn` 挂载，在每天凌晨 3:00 自动触发。
* **冷数据定义与空间释放**：
* 凡是发布时间超过 7 天（`published_at < CURRENT_DATE - INTERVAL '7 days'`）且 **没有任何人类或 LLM 预测与其关联** 的新闻，均被定义为冷数据。
* **清理逻辑**：Worker 通过单条原子 SQL 彻底清空其 `content`（正文全文）字段，将其置为 `NULL`。新闻的 `title` 和 `url` 保持不变，留作历史去重与索引依托。


* **Rust 常驻任务核心代码**：
```rust
pub async fn start_cleanup_tracker(pool: sqlx::PgPool, hot_days: i64) {
    // 强绑定 UTC 时区进行天级对齐
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(86400)); 
    loop {
        interval.tick().await;
        let result = sqlx::query!(
            "UPDATE news SET content = NULL WHERE published_at < (NOW() AT TIME ZONE 'UTC' - make_interval(days => $1)) \
             AND NOT EXISTS (SELECT 1 FROM predictions WHERE predictions.news_id = news.id);",
            hot_days as i32
        ).execute(&pool).await;
        match result {
            Ok(res) => tracing::info!("冷数据清理完成，影响行数: {}", res.rows_affected()),
            Err(e) => tracing::error!("冷数据清理异常: {:?}", e),
        }
    }
}

```



---

## 三、数据库设计

### 3.1 表结构

#### `users` (真实人类账户)

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| id | SERIAL PRIMARY KEY | 主键自增 |
| name | TEXT NOT NULL UNIQUE | 用户名（唯一） |
| role | TEXT NOT NULL CHECK (role IN ('human_expert', 'admin')) | 角色限定：专家、管理员 |
| password_hash | TEXT NOT NULL | 密码哈希 |
| created_at | TIMESTAMPTZ DEFAULT (now() AT TIME ZONE 'UTC') | 创建时间（标准 UTC） |

#### `sources` (采集源)

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| id | SERIAL PRIMARY KEY | 采集源ID |
| name | TEXT NOT NULL | 采集源名称 |
| url | TEXT NOT NULL | RSS/Atom/API 地址 |
| feed_type | TEXT NOT NULL | 限定：`rss`, `atom`, `api` |
| refresh_interval_sec | INT DEFAULT 300 | 动态刷新间隔（秒） |
| enabled | BOOLEAN DEFAULT true | 是否启用 |
| require_keywords | TEXT[] | 静态包含词列表 |
| exclude_keywords | TEXT[] | 静态排除词列表 |

#### `news` (新闻表)

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| id | SERIAL PRIMARY KEY | 新闻唯一ID |
| source_id | INT REFERENCES sources(id) ON DELETE CASCADE | 关联源 |
| title | TEXT NOT NULL | 新闻标题 |
| description | TEXT UNIQUE NOT NULL | 新闻摘要 |
| content | TEXT | 新闻正文（无关联预测时定期置为NULL） |
| url | TEXT UNIQUE | 原始链接（用于库级强去重） |
| published_at | TIMESTAMPTZ NOT NULL | 发布时间（带时区） |
| fetched_at | TIMESTAMPTZ DEFAULT (now() AT TIME ZONE 'UTC') | 抓取时间 |

#### `predictions` (预测解耦表)

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| id | SERIAL PRIMARY KEY | 预测唯一ID |
| news_id | INT REFERENCES news(id) ON DELETE CASCADE | 关联新闻 |
| user_id | INT REFERENCES users(id) ON DELETE SET NULL | 人类预测时必填；LLM 预测时此列保持为 `NULL` |
| prediction_type | TEXT NOT NULL CHECK (prediction_type IN ('human', 'llm')) | 预测主体类型 |
| extracted_facts | TEXT | 摘取事实 |
| inference | TEXT NOT NULL | 逻辑推演过程 |
| probability | DECIMAL(5,2) NOT NULL CHECK (probability BETWEEN 0.00 AND 100.00) | 主观胜率预测 |
| position_size_pct | DECIMAL(5,2) NOT NULL CHECK (position_size_pct BETWEEN 0.00 AND 100.00) | 虚拟仓位，人类强校验必填，用于反脆弱建模 |
| rule_json | JSONB | 可执行判定规则（人类提交，LLM 关联继承） |
| outcome | INT CHECK (outcome IN (0, 1)) | 结果：0证伪，1证实，未到期为 `NULL` |
| parent_prediction_id | INT REFERENCES predictions(id) ON DELETE CASCADE | 指向人类预测ID（LLM 对照组专属） |
| target_date | DATE NOT NULL | 证伪截止日：到期若未触发规则由状态机强行批量判 0 |
| judge_status | TEXT DEFAULT 'pending' CHECK (judge_status IN ('pending', 'judging', 'resolved', 'failed_api')) | 判定状态机控制字（failed_api 含48h自动判负保护） |
| post_mortem | TEXT | 行为纠偏：认知偏误预测强制要求的反思文本 |
| llm_skip | BOOLEAN DEFAULT false | 是否触发熔断跳过 LLM 对照组生成 |
| submitted_at | TIMESTAMPTZ DEFAULT (now() AT TIME ZONE 'UTC') | 提交时间 |
| verified_at | TIMESTAMPTZ | 判定落定时间（带时区） |

#### `noise_flags` (噪声标记反馈)

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| id | SERIAL PRIMARY KEY | 标记ID |
| news_id | INT REFERENCES news(id) ON DELETE CASCADE | 关联噪声新闻 |
| user_id | INT REFERENCES users(id) ON DELETE CASCADE | 标记用户 |
| flagged_at | TIMESTAMPTZ DEFAULT (now() AT TIME ZONE 'UTC') | 标记时间 |

#### `brier_history` (反脆弱统计快照)

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| id | SERIAL PRIMARY KEY | 快照ID |
| user_id | INT REFERENCES users(id) ON DELETE CASCADE | 归属人类用户 |
| calculation_time | TIMESTAMPTZ DEFAULT (now() AT TIME ZONE 'UTC') | 生成时间 |
| human_brier | DECIMAL(10,4) NOT NULL | 人类累计 Brier 分数 |
| llm_brier | DECIMAL(10,4) NOT NULL | 对照组 LLM 累计 Brier 分数 |
| asymmetry_ratio | DECIMAL(10,4) NOT NULL | 不对称比（目标 > 1.5） |
| delta | DECIMAL(10,4) GENERATED ALWAYS AS (human_brier - llm_brier) STORED | 生成列：负数代表人类认知击败模型 |

---

### 3.2 优化后的索引设计

```sql
-- 1. 时间流查询与去重防御索引（强制基于 UTC 检索加速）
CREATE INDEX idx_news_published_at ON news(published_at DESC);
CREATE UNIQUE INDEX idx_news_url_unique ON news(url);

-- 2. 自动化判定器（Judge Worker）部分索引：将到期和挂起的 failed_api 扫描锁定在极小开销范围
CREATE INDEX idx_predictions_active_judge ON predictions(judge_status, target_date) 
WHERE outcome IS NULL;

-- 3. LLM 预测持久化队列扫描加速索引 (用于高效实施 FOR UPDATE SKIP LOCKED)
CREATE INDEX idx_predictions_llm_queue_scan ON predictions(prediction_type, id) 
WHERE llm_skip = false;

-- 4. 依赖 Join 与统计流加速索引
CREATE INDEX idx_predictions_parent_link ON predictions(parent_prediction_id) 
WHERE parent_prediction_id IS NOT NULL;
CREATE INDEX idx_predictions_news_type ON predictions(news_id, prediction_type);
CREATE INDEX idx_noise_flags_lookup ON noise_flags(news_id, flagged_at);

```

---

## 四、前端设计

### 4.1 技术栈

* **HTML5** + **Tailwind CSS v3** (CDN) + **Alpine.js v3** (响应式表单与状态绑定) + **Chart.js** (反脆弱可视化)

### 4.2 核心页面级防呆约束

#### 4.2.1 新闻列表页

* 卡片右上角设置 **“🗑️ 标记为噪音”** 按钮。点击后触发 `POST /api/news/{id}/noise`，并在前端以淡出动画移除卡片。

#### 4.2.2 新闻详情页（决策区）

* 下半区推演表单内置 **强校验机制**：
* 虚拟仓位滑块（0-100%）默认不设初始值，用户必须手动拖动激活。若直接提交，前端阻止并提示：*“虚拟仓位为反脆弱训练核心指标，必须填写！”*
* 预期兑现日限制最小可选日期为 **`当前系统 UTC 日期 + 1天`**，从前端阻断任何无效、追溯或跨时区错位下注。



#### 4.2.3 统计面板页

* **顶部不对称比卡片**：展示 `ASYM` 指标，低于 1.5 时显示黄色警告，提示“仓位管理过于平均，处于脆弱状态”。
* **反脆弱气泡图**：X轴为概率（0-100%），Y轴为实际结果（0或1，加入微小的散点抖动抖散混淆），气泡半径代表仓位。右上角象限（高置信度+重仓+错误结果）被显式标红圈，直观暴露高危脆弱下注。
* **待复盘列表强制约束**：若用户存在未复盘的偏误预测，页面顶部会悬浮红色强警告通知。点击“填写复盘”，弹出模态框提交 `post_mortem` 文本，成功后方可解锁统计卡片，否则全局红标锁定。

---

## 五、判定规则完整示例

### 5.1 价格涨跌幅 (`price_change`)

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

### 5.2 央行利率决议 (`central_bank`)

```json
{
  "type": "central_bank",
  "bank": "fomc",
  "meeting_date": "2026-06-12",
  "expected_action": "hike_25bp"
}

```

### 5.3 URL 关键词检测 (`url_keyword`)

```json
{
  "type": "url_keyword",
  "url": "https://www.example.com/result",
  "keywords": ["approved", "positive"],
  "match_all": false
}

```

### 5.4 经济数据对比 (`economic_data`)

```json
{
  "type": "economic_data",
  "indicator": "cpi",
  "country": "us",
  "release_date": "2026-06-10",
  "expected_value": 3.2,
  "operator": "greater",
  "actual_value_source": "manual",
  "actual_value": 3.5
}

```

---

## 六、配置文件 (`config.toml`)

```toml
[server]
host = "127.0.0.1"
port = 8080
timezone = "UTC"          # 强制系统服务生命周期使用标准化 UTC

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
failed_api_grace_period_hours = 48 # API 判定异常记录的软着陆复试缓冲期
yahoo_finance_base_url = "https://query1.finance.yahoo.com/v8/finance/chart/"

[storage]
hot_days = 7              # 超过7天且无预测的新闻自动清空正文内容
cleanup_cron = "0 3 * * *" # 每天凌晨3点（UTC时间）执行冷热分离Worker

```

---

## 七、完整开发路线图

| 步骤 | 模块 | 具体工程任务 |
| --- | --- | --- |
| 1 | 项目骨架搭建 | 初始化 `cargo new`，配置 `axum`, `sqlx`, `tokio`, `serde`, `rss`, **`jieba-rs` (分词依赖)** 基础依赖。 |
| 2 | 数据库迁移落地 | 编写 `migrations/001_initial.sql`，建立 6 张核心表并部署针对持久化扫描优化的部分索引。 |
| 3 | 采集器基础构建 | 实现 RSS/Atom 解析与异步定时拉取机制，在入库级通过 URL 实施唯一性去重防御。 |
| 4 | 新闻核心 API | 完成 `GET /api/news` 分页流与 `GET /api/news/{id}` 接口开发。 |
| 5 | 前端骨架实现 | 构建 `index.html` 列表页，引入 Alpine.js 动态渲染新闻卡片，打通“🗑️ 标记为噪音”按钮。 |
| 6 | 前端详情决策区 | 实现新闻详情展示，渲染推演表单，对仓位滑块和 **UTC 日期选择** 完成前端硬校验。 |
| 7 | 预测接收模块 | 开发 `POST /api/predictions`，强制校验并留存人类专家提交的预测数据。 |
| 8 | 判定状态机核心 | 部署自动化判定 Worker，编写 **集成 48 小时异常缓冲与全局 UTC 校准的自动判负同步事务**。 |
| 9 | LLM 异步工作流 | 编写基于 **`FOR UPDATE SKIP LOCKED` 状态流转的持久化 Worker**，嵌入三道前置熔断控制链。 |
| 10 | 基础 Brier 统计 | 编写底层统计函数，输出人类与大模型的 Brier 对抗差异，生成日历史快照表。 |
| 11 | 反脆弱高阶指标 | 编写不对称比计算、动态净值曲线、气泡图散点数据生成三个高阶核心统计 API。 |
| 12 | 冷热分离 Worker | 实现凌晨 3:00 异步扫描任务，利用 `NOT EXISTS` 机制安全剔除历史无预测新闻的 content 全文。 |
| 13 | 认知纠偏前端落地 | 在前端面板绘制反脆弱气泡图与不对称比警告卡片，上线高置信度错误“待复盘黑名单”硬约束。 |
| 14 | 判定器全面扩展 | 依次对 `price_change` (Yahoo API), `central_bank`, `economic_data` 进行具体解析器的填充。 |
| 15 | 异常流人工中控面 | 提供手动 outcome 修正端点，开发管理员中控页面，处理挂起即将到期的 `failed_api` 故障记录。 |

---

## 八、核心理念与技术落地映射表

| 核心理念 | 技术落地 |
| --- | --- |
| **反身性理论** | 人类先盲测提交，再触发 LLM；`FOR UPDATE SKIP LOCKED` 持久化任务队列，避免服务重启导致对抗链断裂。 |
| **行为金融学** | 用户“标记噪音” → 分词抽词 → 动态热加载停用词库；`jieba-rs` + `Arc<RwLock<HashSet<String>>>`，采集器免重启进化。 |
| **证伪主义** | 强制 `target_date`，到期未触发判定规则即批量判负；全局 UTC 时区锁定 + `CURRENT_TIMESTAMP AT TIME ZONE 'UTC'` 原子事务。 |
| **反脆弱原则** | 异常缓冲 48 小时（避免 API 故障误判）；气泡图带 Jitter 防混淆，复盘表单强约束，暴露不对称下注。 |
| **冷热分离** | 7 天后无人下注的新闻正文物理置 `NULL`；`NOT EXISTS` 子查询 + Tokio 定时 Worker，释放 90% 存储空间。 |