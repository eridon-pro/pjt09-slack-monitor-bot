import sqlite3

# DB 初期化: user_scores と events テーブルを生成
conn = sqlite3.connect("scores.db")
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS user_scores (
    user_id TEXT PRIMARY KEY,
    post_count INTEGER DEFAULT 0,
    positive_count INTEGER DEFAULT 0,
    violation_count INTEGER DEFAULT 0
)
"""
)
cur.execute("""
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    type TEXT NOT NULL,        -- 'post' / 'positive' / 'violation'
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
)
conn.commit()
conn.close()
print("Initialized scores.db with user_scores and events tables.")
