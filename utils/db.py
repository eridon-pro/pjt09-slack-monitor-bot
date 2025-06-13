import sqlite3


def update_score(user_id: str,
                 post=False,
                 reaction=False,
                 answer=False,
                 positive_feedback=False,
                 violation=False):
    conn = sqlite3.connect("scores.db")
    cur = conn.cursor()
    # 累計テーブル更新
    cur.execute("INSERT OR IGNORE INTO user_scores(user_id) VALUES(?)", (user_id,))
    if post:
        cur.execute("UPDATE user_scores SET post_count = post_count + 1 WHERE user_id = ?", (user_id,))
    if reaction:
        cur.execute("UPDATE user_scores SET reaction_count = reaction_count + 1 WHERE user_id = ?", (user_id,))
    if answer:
        cur.execute("UPDATE user_scores SET answer_count = answer_count + 1 WHERE user_id = ?", (user_id,))
    if positive_feedback:
        cur.execute("UPDATE user_scores SET positive_feedback_count = positive_feedback_count + 1 WHERE user_id = ?", (user_id,))
    if violation:
        cur.execute("UPDATE user_scores SET violation_count = violation_count + 1 WHERE user_id = ?", (user_id,))
    # events にも履歴を残す
    for t, flag in [("post", post), ("reaction", reaction), ("answer", answer), ("positive_feedback", positive_feedback), ("violation", violation)]:
        if flag:
            cur.execute("INSERT INTO events(user_id, type) VALUES(?,?)", (user_id, t))
    conn.commit()
    conn.close()
