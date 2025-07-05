import os
import time
import datetime
from datetime import timedelta
import json
import sqlite3
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

from dotenv import load_dotenv
load_dotenv()
DB_PATH = os.environ.get("SCORES_DB_PATH", "scores.db")


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
    reaction_name: str = None,
    violation_rule: str = None
):
    """
    任意イベントをeventsテーブルに記録する汎用関数。

    :param user_id: イベント対象ユーザー（例: 投稿者、違反者、回答者、被リアクション者）
    :param event_type: イベント種別（'post', 'reaction', 'answer', 'positive_feedback', 'violation'等）
    :param ts_epoch: イベント時刻（float: UNIXエポック秒、未指定なら現在時刻）
    :param reactor_id: リアクションした人（通常イベントではNone）
    :param reaction_name: リアクション名（通常イベントではNone）
    :param violation_rule: ガイドライン違反と判定された時の該当するルール番号（通常イベントではNone）
    """
    if ts_epoch is None:
        ts_epoch = time.time()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO events (user_id, reactor_id, type, reaction_name, ts_epoch, violation_rule) VALUES (?, ?, ?, ?, ?, ?)",
        (user_id, reactor_id, event_type, reaction_name, ts_epoch, violation_rule)
    )
    event_id = cur.lastrowid
    conn.commit()
    logger.info(
        f"DB INSERT: user_id={user_id}, reactor_id={reactor_id}, type={event_type}, reaction_name={reaction_name}, ts_epoch={ts_epoch}"
    )
    conn.close()
    return event_id


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


def fetch_posts_for_faq(conn, channel, window_days=7):
    """
    指定チャンネルの、最近window_days日以内の質問（親スレッド）投稿(slack_posts)を返す(id, ts, text, thread_ts)。
    """
    now = datetime.datetime.utcnow().timestamp()
    window_start = now - window_days * 86400
    cur = conn.cursor()
    cur.execute(
        "SELECT id, ts, text, thread_ts FROM slack_posts WHERE channel=? AND ts>=? AND thread_ts=ts",
        (channel, window_start)
    )
    #return cur.fetchall()
    rows = cur.fetchall()
    return [
        {"id": r[0], "ts": r[1], "text": r[2], "thread_ts": r[3]}
        for r in rows
    ]


def fetch_posts_for_topics(conn, channels, window_days=7):
    """
    指定チャンネルリストから、最近window_days日以内のトップレベル投稿(slack_posts)を返す(id, ts, text)。
    channels: list of channel IDs
    """
    now = datetime.datetime.utcnow().timestamp()
    window_start = now - window_days * 86400
    cur = conn.cursor()
    placeholders = ','.join('?' for _ in channels)
    query = f"SELECT id, ts, text FROM slack_posts WHERE channel IN ({placeholders}) AND ts>=? AND thread_ts=ts"
    cur.execute(query, (*channels, window_start))
    rows = cur.fetchall()
    return [
        {"id": r[0], "ts": r[1], "text": r[2]}
        for r in rows
    ]


def insert_extracted_item(post_ids, title, created_at=None, answer=None, source_url=None):
    """
    extracted_itemsテーブルに新規アイテムを挿入する。
    post_idsはリスト、titleは文字列
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        if created_at is None:
            created_at = datetime.datetime.utcnow().isoformat()
        item_json = {"post_ids": post_ids, "title": title}
        cur.execute(
            "INSERT INTO extracted_items (post_ids, title, created_at, answer, source_url) VALUES (?, ?, ?, ?, ?)",
            (json.dumps(post_ids), title, created_at, answer, source_url)
        )
        item_id = cur.lastrowid
        conn.commit()
        logger.info(f"Inserted extracted_item with id={item_id}, title={title}")
        return item_id
    except Exception as e:
        logger.error(f"Failed to insert extracted_item: {e}")
        raise
    finally:
        conn.close()


def insert_extracted_item_type(item_id, type_name):
    """
    extracted_item_typesテーブルに新規タイプを挿入する。
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO extracted_item_types (item_id, type) VALUES (?, ?)",
            (item_id, type_name)
        )
        conn.commit()
        logger.info(f"Inserted extracted_item_type with item_id={item_id}, type_name={type_name}")
    except Exception as e:
        logger.error(f"Failed to insert extracted_item_type: {e}")
        raise
    finally:
        conn.close()


def insert_trend_topic(conn, label, topic_text, size, created_at=None):
    """
    trend_topics テーブルに新規トピックを挿入する。
    """
    if created_at is None:
        created_at = datetime.datetime.utcnow().timestamp()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO trend_topics (label, topic_text, size, created_at) VALUES (?, ?, ?, ?)",
        (label, topic_text, size, created_at)
    )
    conn.commit()
    return cur.lastrowid


def insert_info_request(conn, label: int, requests: str, size: int, created_at=None) -> int:
    """
    info_requests テーブルに新規情報リクエストを挿入する。
    """
    if created_at is None:
        created_at = datetime.datetime.utcnow().timestamp()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO info_requests (label, request_text, size, created_at) VALUES (?, ?, ?, ?)",
        (label, requests, size, created_at)
    )
    conn.commit()
    return cur.lastrowid


def fetch_last_import_ts(conn) -> float:
    """
    Retrieve the last import timestamp from the import_state table.
    If no timestamp is found, returns 0.
    """
    cur = conn.cursor()
    cur.execute("SELECT last_ts FROM import_state ORDER BY last_ts DESC LIMIT 1")
    row = cur.fetchone()
    return row[0] if row else 0.0


def fetch_thread_replies(conn, post_id):
    """
    Fetch the texts of all replies in the thread of the given post ID.
    Returns a list of strings (text of replies).
    """
    cur = conn.cursor()
    # First, get the thread_ts for the given post_id
    cur.execute("SELECT thread_ts FROM slack_posts WHERE id = ?", (post_id,))
    row = cur.fetchone()
    if row is None or row[0] is None:
        return []
    thread_ts = row[0]
    # Now fetch all posts in this thread (excluding the main post)
    cur.execute(
        "SELECT text FROM slack_posts WHERE thread_ts = ? AND id != ? ORDER BY ts ASC",
        (thread_ts, post_id)
    )
    replies = [r[0] for r in cur.fetchall()]
    return replies


def fetch_post_text(conn, post_id):
    """
    Fetch the text of a single post by its ID.
    Returns the text string, or None if not found.
    """
    cur = conn.cursor()
    cur.execute("SELECT text FROM slack_posts WHERE id = ?", (post_id,))
    row = cur.fetchone()
    if row is None:
        return None
    return row[0]


def count_posts_since(conn, ts: float, channels=None, include_replies=False) -> int:
    """
    指定した時刻以降の投稿数をカウントする。
    
    :param conn: データベース接続
    :param ts: 基準タイムスタンプ
    :param channels: 対象チャネルのリスト（Noneなら全チャネル）
    :param include_replies: Trueなら返信も含む、Falseなら親スレッドのみ
    """
    cur = conn.cursor()
    
    # ベースクエリ
    query = "SELECT COUNT(*) FROM slack_posts WHERE ts > ?"
    params = [ts]
    
    # チャネル指定
    if channels:
        placeholders = ','.join('?' for _ in channels)
        query += f" AND channel IN ({placeholders})"
        params.extend(channels)
    
    # 返信を含むかどうか
    if not include_replies:
        query += " AND thread_ts = ts"
    
    cur.execute(query, params)
    return cur.fetchone()[0]
