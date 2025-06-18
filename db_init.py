import sqlite3

# DB 初期化: user_scores と events テーブルを生成
conn = sqlite3.connect("scores.db")
cur = conn.cursor()
# 累計スコア保持
cur.execute("""
CREATE TABLE IF NOT EXISTS user_scores (
    user_id TEXT PRIMARY KEY,
    post_count INTEGER DEFAULT 0,
    reaction_count INTEGER DEFAULT 0,
    answer_count INTEGER DEFAULT 0,
    positive_feedback_count INTEGER DEFAULT 0,
    violation_count INTEGER DEFAULT 0
)
""")

# 時系列イベントログ
cur.execute("""
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,             -- 投稿者
    reactor_id TEXT,                   -- リアクションした人（投稿ならNULL）
    type TEXT NOT NULL,                -- 'post','reaction','answer','positive_feedback','violation'
    reaction_name TEXT,                -- '+1', 'pray' など（リアクション時のみ）
    ts_epoch REAL,
    scored INTEGER DEFAULT 0,          -- 加点済みなら1, 未加点なら0
    violation_rule TEXT DEFAULT NULL,  -- ガイドライン違反と判定されたルール番号
)
""")

# ポジティブリアクションキャッシュ（永続化キャッシュ）
cur.execute("""
CREATE TABLE IF NOT EXISTS reaction_judgement (
    reaction_name TEXT PRIMARY KEY,
    is_positive INTEGER,
    last_checked_ts REAL
)
""")

conn.commit()
conn.close()
print("Initialized scores.db with user_scores, events, and reaction_judgement tables.")
