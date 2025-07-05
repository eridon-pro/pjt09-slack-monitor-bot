import sqlite3

# DB 初期化: user_scores と events テーブルを生成
conn = sqlite3.connect("scores.db")
cur = conn.cursor()
# 累計スコア保持
cur.execute(
    """
CREATE TABLE IF NOT EXISTS user_scores (
    user_id TEXT PRIMARY KEY,
    post_count INTEGER DEFAULT 0,
    reaction_count INTEGER DEFAULT 0,
    answer_count INTEGER DEFAULT 0,
    positive_feedback_count INTEGER DEFAULT 0,
    violation_count INTEGER DEFAULT 0
)
"""
)

# 時系列イベントログ
cur.execute(
    """
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,             -- 投稿者
    reactor_id TEXT,                   -- リアクションした人（投稿ならNULL）
    type TEXT NOT NULL,                -- 'post','reaction','answer','positive_feedback','violation'
    reaction_name TEXT,                -- '+1', 'pray' など（リアクション時のみ）
    ts_epoch REAL,
    scored INTEGER DEFAULT 0,          -- 加点済みなら1, 未加点なら0
    violation_rule TEXT DEFAULT NULL   -- ガイドライン違反と判定されたルール番号
)
"""
)

# ポジティブリアクションキャッシュ（永続化キャッシュ）
cur.execute(
    """
CREATE TABLE IF NOT EXISTS reaction_judgement (
    reaction_name TEXT PRIMARY KEY,
    is_positive INTEGER,
    last_checked_ts REAL
)
"""
)


# PJT10： Slack投稿全件保存
cur.execute(
    """
CREATE TABLE IF NOT EXISTS slack_posts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL    NOT NULL,
    channel     TEXT    NOT NULL,
    user        TEXT    NOT NULL,
    text        TEXT    NOT NULL,
    thread_ts   REAL    NOT NULL,
    item_type   TEXT    DEFAULT NULL
)
"""
)

# PJT10: 抽出結果まとめテーブル
cur.execute(
    """
CREATE TABLE IF NOT EXISTS extracted_items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    post_ids        TEXT    NOT NULL,  -- JSON list of slack_posts.id
    title           TEXT    NOT NULL,  -- 要約文／トピック名／情報リクエスト要約
    created_at      REAL    NOT NULL,
    answer          TEXT    DEFAULT NULL,
    source_url      TEXT    DEFAULT NULL
)
"""
)

# PJT10: 抽出種別タグ付け
cur.execute(
    """
CREATE TABLE IF NOT EXISTS extracted_item_types (
    item_id     INTEGER NOT NULL,  -- FK → extracted_items.id
    type        TEXT    NOT NULL,  -- 'faq','topic','info'
    PRIMARY KEY (item_id, type),
    FOREIGN KEY (item_id) REFERENCES extracted_items(id)
)
"""
)

# PJT10: 最後のPostのTS保存
cur.execute(
    """
CREATE TABLE IF NOT EXISTS import_state (
  key TEXT PRIMARY KEY,
  last_ts REAL
)
"""
)

# PJT10: トレンドトピックまとめテーブル
cur.execute(
    """
CREATE TABLE IF NOT EXISTS trend_topics (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    label       INTEGER NOT NULL,     -- クラスタ番号
    topic_text  TEXT    NOT NULL,     -- 抽出されたトピック名
    size        INTEGER NOT NULL,     -- クラスタの投稿数
    created_at  REAL    NOT NULL      -- 登録時タイムスタンプ (UNIX 秒)
)
"""
)

# PJT10: 情報リクエストまとめテーブル
cur.execute(
    """
CREATE TABLE IF NOT EXISTS info_requests (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    label         INTEGER NOT NULL,    -- クラスタ番号
    request_text  TEXT    NOT NULL,    -- 抽出された情報リクエスト要約
    size          INTEGER NOT NULL,    -- クラスタの投稿数
    created_at    REAL    NOT NULL     -- 登録時タイムスタンプ (UNIX 秒)
)
"""
)

conn.commit()
conn.close()
print(
    "Initialized scores.db with user_scores, events, reaction_judgement, slack_posts, extracted_items, and extracted_item_types tables."
)
