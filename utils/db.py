import os
import sqlite3
import time
import logging
logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("SCORES_DB_PATH", "scores.db")

'''def update_score(
    user_id: str,
    post=False,
    reaction=False,
    answer=False,
    positive_feedback=False,
    violation=False,
    reactor_id=None,
    reaction_name=None,
    ts_epoch=None
):
    """
    スコア集計(user_scores)とイベント履歴(events)を一括更新するユーティリティ関数。

    - 各種アクション（post/reaction/answer/positive_feedback/violation）をboolで指定
    - reaction時のみreactor_id, reaction_nameを記録
    - ts_epoch未指定時は現在時刻（UNIXエポック秒）で記録
    """
    conn = sqlite3.connect(DB_PATH)
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

    now_epoch = ts_epoch if ts_epoch is not None else time.time()  # ここで現在時刻（float: エポック秒）を取得

    # events にも履歴を残す
    for t, flag in [("post", post), ("reaction", reaction), ("answer", answer), ("positive_feedback", positive_feedback), ("violation", violation)]:
        if flag:
            # reaction の時だけ reactor_id, reaction_name を記録
            if t == "reaction":
                logger.info(f"DB INSERT: user_id={user_id}, reactor_id={reactor_id}, type={t}, reaction_name={reaction_name}, ts_epoch={now_epoch}")  # Debug
                cur.execute(
                    "INSERT INTO events(user_id, reactor_id, type, reaction_name, ts_epoch) VALUES(?,?,?,?,?)",
                    (user_id, reactor_id, t, reaction_name, now_epoch)
                )
            else:
                cur.execute(
                    "INSERT INTO events(user_id, reactor_id, type, reaction_name, ts_epoch) VALUES(?,?,?,?,?)",
                    (user_id, None, t, None, now_epoch)
                )
    conn.commit()
    conn.close()'''


def update_score(
    user_id: str,
    post=False,
    reaction=False,
    answer=False,
    positive_feedback=False,
    violation=False
):
    """
    スコア(user_scores)を更新するユーティリティ関数。
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
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
    conn.commit()
    conn.close()

'''def record_reaction_event(user_id, reactor_id, reaction_name, ts_epoch):
    """
    reactionをDB eventsに記録するユーティリティ関数。
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO events(user_id, reactor_id, type, reaction_name, ts_epoch) VALUES (?, ?, ?, ?, ?)",
        (user_id, reactor_id, "reaction", reaction_name, ts_epoch)
    )
    conn.commit()
    conn.close()'''


def record_event(
    user_id: str,
    event_type: str,
    ts_epoch: float = None,
    reactor_id: str = None,
    reaction_name: str = None
):
    """
    任意イベントをeventsテーブルに記録する汎用関数。

    :param user_id: イベント対象ユーザー（例: 投稿者、違反者、回答者、被リアクション者）
    :param event_type: イベント種別（'post', 'reaction', 'answer', 'positive_feedback', 'violation'等）
    :param ts_epoch: イベント時刻（float: UNIXエポック秒、未指定なら現在時刻）
    :param reactor_id: リアクションした人（通常イベントではNone）
    :param reaction_name: リアクション名（通常イベントではNone）
    """
    if ts_epoch is None:
        ts_epoch = time.time()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO events (user_id, reactor_id, type, reaction_name, ts_epoch) VALUES (?, ?, ?, ?, ?)",
        (user_id, reactor_id, event_type, reaction_name, ts_epoch)
    )
    conn.commit()
    conn.close()
    logger.info(
        f"DB INSERT: user_id={user_id}, reactor_id={reactor_id}, type={event_type}, reaction_name={reaction_name}, ts_epoch={ts_epoch}"
    )


def is_positive_reaction(reaction_name: str):
    """
    指定リアクションがポジティブか（キャッシュ）DBから判定。
    戻り値: True(1), False(0), 未判定(None)
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT is_positive FROM reaction_judgement WHERE reaction_name = ?", (reaction_name,)
    )
    row = cur.fetchone()
    conn.close()
    if row is None:
        return None
    return bool(row[0])


def cache_positive_reaction(reaction_name: str, is_positive: bool):
    """
    リアクションに対するポジティブ/非ポジティブ判定をキャッシュ(DB)に記録
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO reaction_judgement (reaction_name, is_positive, last_checked_ts) VALUES (?, ?, ?)",
        (reaction_name, int(is_positive), time.time())
    )
    conn.commit()
    conn.close()


def get_unjudged_reactions():
    """
    reaction_judgementテーブルに未登録のリアクション名一覧を返す。
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT reaction_name FROM events
        WHERE type='reaction'
          AND reaction_name IS NOT NULL
          AND reaction_name NOT IN (SELECT reaction_name FROM reaction_judgement)
    """)
    rows = cur.fetchall()
    conn.close()
    return [row[0] for row in rows if row[0]]


def get_unscored_positive_reactions():
    """
    reaction_judgementでポジティブと判定済み、かつscored=0のreactionイベントを返す
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT events.id, events.user_id, events.reactor_id, events.reaction_name, events.ts_epoch
        FROM events
        JOIN reaction_judgement ON events.reaction_name = reaction_judgement.reaction_name
        WHERE events.type = 'reaction'
          AND events.scored = 0
          AND reaction_judgement.is_positive = 1
    """)
    rows = cur.fetchall()
    conn.close()
    # カラム順に注意
    return [
        {
            "id": row[0],
            "user_id": row[1],
            "reactor_id": row[2],
            "reaction_name": row[3],
            "ts_epoch": row[4],
        }
        for row in rows
    ]


def mark_reaction_scored(event_id: int):
    """
    イベントIDをscored=1にする
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "UPDATE events SET scored=1 WHERE id=?",
        (event_id,)
    )
    conn.commit()
    conn.close()


def apply_reaction_scores(events):
    """
    まとめてリアクション加点（eventsはget_positive_reaction_events()などの結果）。
    user_id（=リアクション付与された人）に対して加点し、scoredフラグを立てる。
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for event in events:
        # 加点処理
        cur.execute("UPDATE user_scores SET reaction_count = reaction_count + 1 WHERE user_id = ?", (event["user_id"],))
        # scoredフラグを立てる
        cur.execute("UPDATE events SET scored=1 WHERE id=?", (event["id"],))
    conn.commit()
    conn.close()
