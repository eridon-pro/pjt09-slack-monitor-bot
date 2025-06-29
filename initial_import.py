import os, time, sqlite3, logging
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

MAX_RETRIES = 3

load_dotenv()
SLACK_TOKEN   = os.environ["SLACK_BOT_TOKEN"]
QUESTION_CH   = os.environ["QUESTION_CHANNEL"]
DEV_CH        = os.environ["BOT_DEV_CHANNEL"]       # bot-dev・bot-qa-dev を指定
DB_PATH       = "scores.db"

logging.basicConfig(level=logging.INFO)
slack = WebClient(token=SLACK_TOKEN)

def fetch_all_threads(channel: str, oldest_ts: float = 0.0):
    """Slack→過去全スレッド先頭メッセージ(ts,user,text)を返す"""
    cursor, all_msgs = None, []
    while True:
        retry_count = 0
        while True:
            try:
                resp = slack.conversations_history(
                    channel=channel,
                    limit=50,
                    cursor=cursor,
                    oldest=oldest_ts
                )
                break
            except SlackApiError as e:
                if e.response.get("error") == "ratelimited" and retry_count < MAX_RETRIES:
                    retry_after = int(e.response.headers.get("Retry-After", 2))
                    logging.info(f"Rate limited fetching history for {channel}, retrying after {retry_after}s")
                    time.sleep(retry_after)
                    retry_count += 1
                    continue
                else:
                    raise
        time.sleep(1.2)
        all_msgs += resp["messages"]
        cursor = resp.get("response_metadata",{}).get("next_cursor")
        if not cursor:
            break
    # スレッド先頭のみ残す
    return [
        (m["ts"], m.get("user",""), m.get("text","").strip())
        for m in all_msgs
        if m.get("thread_ts",m["ts"]) == m["ts"]
           and m.get("subtype") != "bot_message"
    ]

def import_posts(db, entries):
    """slack_posts にバルクインサート"""
    cur = db.cursor()
    cur.executemany(
      "INSERT OR IGNORE INTO slack_posts(ts, channel, user, text, thread_ts) VALUES(?,?,?,?,?)",
      entries
    )
    db.commit()

def main():
    from pipelines import process_faq, process_trend_topics, process_info_requests

    db = sqlite3.connect(DB_PATH)
    # 1) FAQ 元データ(bot-qa-dev)
    qa_threads = fetch_all_threads(QUESTION_CH, oldest_ts=0)
    logging.info(f"Fetched {len(qa_threads)} faq threads")
    import_posts(db, [(ts, QUESTION_CH, user, text, ts) for ts, user, text in qa_threads])

    # スレッド内のリプライ（回答）を取り込む
    for thread_ts, _, _ in qa_threads:
        retry_count = 0
        while True:
            try:
                resp = slack.conversations_replies(channel=QUESTION_CH, ts=thread_ts, limit=100)
                # 先頭メッセージは質問なので除外し、サブタイプが bot_message でないものを回答として登録
                replies = [
                    (m["ts"], QUESTION_CH, m.get("user", ""), m.get("text", "").strip(), thread_ts)
                    for m in resp.get("messages", [])[1:]
                    if m.get("subtype") != "bot_message"
                ]
                if replies:
                    import_posts(db, replies)
                break
            except SlackApiError as e:
                if e.response.get("error") == "ratelimited" and retry_count < MAX_RETRIES:
                    retry_after = int(e.response.headers.get("Retry-After", 2))
                    logging.info(f"Rate limited when fetching replies for thread {thread_ts}, retrying after {retry_after}s")
                    time.sleep(retry_after)
                    retry_count += 1
                    continue
                else:
                    logging.warning(f"Failed fetching replies for thread {thread_ts}: {e}")
                    break

    # 2) トレンド候補(bot-dev + bot-qa-dev)
    trend_threads = fetch_all_threads(DEV_CH, oldest_ts=0)
    logging.info(f"Fetched {len(trend_threads)} trend threads")
    import_posts(db, [(ts, DEV_CH, user, text, ts) for ts, user, text in trend_threads])

    # 分類パイプラインを起動
    process_faq(db)
    process_trend_topics(db)
    process_info_requests(db)

    db.close()
    logging.info("初回インポート完了")

if __name__ == "__main__":
    main()
