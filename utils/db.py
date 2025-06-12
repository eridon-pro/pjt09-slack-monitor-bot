import sqlite3

def update_score(user_id: str, positive: bool=False, violation: bool=False):
    conn = sqlite3.connect("scores.db")
    cur = conn.cursor()
    # user_scores テーブル更新
    cur.execute(
        "INSERT OR IGNORE INTO user_scores(user_id) VALUES(?)",
        (user_id,)
    )
    cur.execute(
        "UPDATE user_scores SET post_count = post_count + 1 WHERE user_id = ?",
        (user_id,)
    )
    if positive:
        cur.execute(
            "UPDATE user_scores SET positive_count = positive_count + 1 WHERE user_id = ?",
            (user_id,)
        )
    if violation:
        cur.execute(
            "UPDATE user_scores SET violation_count = violation_count + 1 WHERE user_id = ?",
            (user_id,)
        )
    # events テーブルに履歴を残す
    cur.execute(
        "INSERT INTO events(user_id, type) VALUES(?, 'post')",
        (user_id,)
    )
    if positive:
        cur.execute(
            "INSERT INTO events(user_id, type) VALUES(?, 'positive')",
            (user_id,)
        )
    if violation:
        cur.execute(
            "INSERT INTO events(user_id, type) VALUES(?, 'violation')",
            (user_id,)
        )
    conn.commit()
    conn.close()
