import os
import time
import sqlite3
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

from datetime import datetime, timezone
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from pipelines import process_faq, process_trend_topics, process_info_requests
from publishers import (
    post_faq_to_slack,
    post_trends_to_slack,
    post_info_requests_to_slack,
    notion_upsert_all,
)

from dotenv import load_dotenv

load_dotenv()
SLACK_TOKEN = os.environ["SLACK_BOT_TOKEN"]
QUESTION_CH = os.environ["QUESTION_CHANNEL"]  # bot-qa-dev
DEV_CH = os.environ["BOT_DEV_CHANNEL"]  # bot-dev
DB_PATH = os.getenv("SCORES_DB_PATH", "scores.db")

MAX_RETRIES = 3
RATE_LIMIT_SLEEP = 1.2  # per‐call spacing

slack = WebClient(token=SLACK_TOKEN)


def get_max_post_ts(db):
    """slack_posts テーブルの最大 ts を返す（なければ None）"""
    cur = db.cursor()
    cur.execute(
        """
      SELECT MAX(ts) FROM slack_posts
    """
    )
    row = cur.fetchone()
    return row[0] if row and row[0] is not None else None


def get_last_import_ts(db):
    """前回の取得時刻を返す（なければ None）"""
    cur = db.cursor()
    cur.execute(
        """
      CREATE TABLE IF NOT EXISTS import_state (
        key TEXT PRIMARY KEY,
        last_ts REAL
      )
    """
    )
    cur.execute("SELECT last_ts FROM import_state WHERE key = 'daily_import'")
    row = cur.fetchone()
    return row[0] if row else None


def update_last_import_ts(db, ts):
    """import_state を UPSERT して当日時刻を保存"""
    cur = db.cursor()
    cur.execute(
        """
      INSERT INTO import_state(key, last_ts) VALUES('daily_import', ?)
      ON CONFLICT(key) DO UPDATE SET last_ts=excluded.last_ts
    """,
        (ts,),
    )
    db.commit()


def fetch_threads(channel: str, oldest_ts: float):
    """指定 channel で thread_ts==ts の“親スレッド”のみ差分取得"""
    cursor = None
    all_msgs = []
    while True:
        retry = 0
        while True:
            try:
                resp = slack.conversations_history(
                    channel=channel, oldest=oldest_ts, limit=50, cursor=cursor
                )
                break
            except SlackApiError as e:
                if e.response.get("error") == "ratelimited" and retry < MAX_RETRIES:
                    retry_after = int(e.response.headers.get("Retry-After", 60))
                    logging.info(
                        f"Rate limited on history {channel}, retry after {retry_after}s"
                    )
                    time.sleep(retry_after)
                    retry += 1
                    continue
                else:
                    raise
        time.sleep(RATE_LIMIT_SLEEP)
        all_msgs.extend(resp["messages"])
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break

    # 親スレッドのみ抽出、bot メッセージは無視
    return [
        (m["ts"], channel, m.get("user", ""), m.get("text", "").strip(), m["ts"])
        for m in all_msgs
        if m.get("thread_ts", m["ts"]) == m["ts"] and m.get("subtype") != "bot_message"
    ]


def fetch_replies(channel: str, thread_ts: str):
    """一つの thread_ts に対して replies を取得"""
    retry = 0
    while True:
        try:
            resp = slack.conversations_replies(channel=channel, ts=thread_ts, limit=100)
            break
        except SlackApiError as e:
            if e.response.get("error") == "ratelimited" and retry < MAX_RETRIES:
                retry_after = int(e.response.headers.get("Retry-After", 60))
                logging.info(
                    f"Rate limited on replies {thread_ts}, retry after {retry_after}s"
                )
                time.sleep(retry_after)
                retry += 1
                continue
            else:
                logging.warning(f"Failed fetching replies for {thread_ts}: {e}")
                return []

    time.sleep(RATE_LIMIT_SLEEP)
    # 先頭は質問なので除外、bot_message も除外
    return [
        (m["ts"], channel, m.get("user", ""), m.get("text", "").strip(), thread_ts)
        for m in resp.get("messages", [])[1:]
        if m.get("subtype") != "bot_message"
    ]


def import_posts(db, entries):
    """slack_posts テーブルへのバルクインサート"""
    cur = db.cursor()
    cur.executemany(
        "INSERT OR IGNORE INTO slack_posts(ts, channel, user, text, thread_ts) VALUES (?, ?, ?, ?, ?)",
        entries,
    )
    db.commit()


def fetch_and_import(db, channel, oldest_ts):
    # 1) 親スレッド取得＋インサート
    threads = fetch_threads(channel, oldest_ts)
    logging.info(f"{channel}: fetched {len(threads)} threads since {oldest_ts}")
    import_posts(db, threads)

    # 2) 各スレッドの replies 取得＋インサート
    for ts, _, _, _, thread_ts in threads:
        replies = fetch_replies(channel, thread_ts)
        if replies:
            import_posts(db, replies)


def fetch_threads_only(db, channel, oldest_ts):
    """親スレッドのみ取得してインサート（replies は取得しない）"""
    threads = fetch_threads(channel, oldest_ts)
    logging.info(
        f"{channel}: fetched {len(threads)} threads since {oldest_ts} (threads only)"
    )
    import_posts(db, threads)


def main():
    db = sqlite3.connect(DB_PATH)

    # 前回時刻取得
    last_ts = get_last_import_ts(db)
    logging.info(f"Last import ts = {last_ts}")

    if last_ts is None:
        logging.warning("No previous import timestamp found. Exiting without fetching.")
        db.close()
        return

    # bot-qa-dev の質問と回答
    fetch_and_import(db, QUESTION_CH, last_ts)

    # bot-dev の一般投稿（トレンド候補） - 親スレッドのみ取得
    fetch_threads_only(db, DEV_CH, last_ts)

    # 分類パイプライン呼び出し（import_stateを更新する前に実行）
    processing_occurred = False
    try:
        if process_faq(db):
            processing_occurred = True
        if process_trend_topics(db):
            processing_occurred = True
        if process_info_requests(db):
            processing_occurred = True

        # Slack投稿は常に実行（既存データがあれば）
        post_faq_to_slack(db)
        post_trends_to_slack(db)
        post_info_requests_to_slack(db)
        notion_upsert_all(db)
    except Exception as e:
        logging.error(f"Error during processing pipelines: {e}")

    # 何らかの処理が実行された場合のみimport_stateを更新
    if processing_occurred:
        interim_ts = get_max_post_ts(db)
        update_last_import_ts(db, interim_ts)
        logging.info(f"Updated import_state (post-pipeline) to {interim_ts}")
    else:
        logging.info("No processing occurred, import_state not updated")

    db.close()
    logging.info("Daily import complete")


if __name__ == "__main__":
    main()
