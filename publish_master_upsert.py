import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
from dotenv import load_dotenv
load_dotenv()

import os
import sqlite3
from datetime import datetime, timedelta
from notion_client import Client
from utils.scoring import fetch_user_counts, compute_score
from utils.slack_helpers import resolve_user

# Set up logger for this module
logger = logging.getLogger(__name__)

# 環境変数
DB_PATH        = os.getenv("SCORES_DB_PATH", "scores.db")
NOTION_DB_ID   = os.getenv("NOTION_DB_ID")
NOTION_TOKEN   = os.getenv("NOTION_TOKEN")
NOTION_PAGE_ID = os.getenv("NOTION_PAGE_ID")  # 画像埋め込みやタイムスタンプ追記用のページID
#NOTION_UPDATED_BLOCK_ID = os.getenv("NOTION_UPDATED_BLOCK_ID")  # 最終更新日を入力するブロックから取得したプレースホルダーブロックのID
SLACK_WORKSPACE_URL = os.getenv("SLACK_WORKSPACE_URL")
TOP_N = int(os.getenv("TOP_N", "5"))


# Notion クライアント初期化
notion = Client(auth=NOTION_TOKEN)

def clear_timestamp_block():
    """
    Delete existing '最終更新:' paragraph blocks under the target page.
    """
    # Retrieve current children blocks
    url = f"https://api.notion.com/v1/blocks/{NOTION_PAGE_ID}/children?page_size=100"
    res = notion.blocks.children.list(block_id=NOTION_PAGE_ID, page_size=100)
    for child in res.get("results", []):
        # Identify timestamp paragraphs by checking type and text content
        if child.get("type") == "paragraph":
            texts = child["paragraph"]["rich_text"]
            if texts and texts[0].get("type") == "text" and texts[0]["text"]["content"].startswith("最終更新:"):
                # Archive (delete) this block
                notion.blocks.update(block_id=child["id"], archived=True)


def clear_all_records():
    """Archive all existing pages in the database."""
    start_cursor = None
    while True:
        resp = notion.databases.query(
            database_id=NOTION_DB_ID,
            start_cursor=start_cursor,
            page_size=100
        )
        for page in resp["results"]:
            notion.pages.update(page_id=page["id"], archived=True)
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")


def update_timestamp_block():
    """
    Notionページの末尾に「最終更新: YYYY-MM-DD HH:MM:SS」を新規追加する。
    """
    # Remove any existing timestamp blocks
    clear_timestamp_block()
    now_iso = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Append a new paragraph block with the timestamp
    notion.blocks.children.append(
        block_id=NOTION_PAGE_ID,  # append to the target Notion page, not the DB ID
        children=[
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": f"最終更新: {now_iso}"}
                        }
                    ]
                }
            }
        ]
    )


def upsert_to_notion(user_id: str, since_tag: str, until_tag: str,
                     posts: int, reactions: int, answers: int,
                     positive_fb: int, violations: int,
                     score: float, period: str,
                     prefix: str = ''):
    display_name = resolve_user(user_id)
    # フィルター条件
    filter_params = {
        "and": [
            {"property": "ユーザー", "title": {"equals": f"@{display_name}"}},
            {"property": "集計開始日", "date": {"equals": since_tag}},
            {"property": "集計終了日", "date": {"equals": until_tag}},
            {"property": "期間", "select": {"equals": period}}
        ]
    }
    existing = notion.databases.query(
        database_id=NOTION_DB_ID,
        filter=filter_params
    ).get("results", [])

    # prepend @ and link to Slack profile with separate prefix fragment if any
    title_fragments = []
    if prefix:
        title_fragments.append({
            "text": {"content": prefix}
        })
    title_fragments.append({
        "text": {
            "content": f"@{display_name}",
            "link": {"url": f"https://{SLACK_WORKSPACE_URL}/team/{user_id}"}
        },
    })

    props = {
        "ユーザー": {"title": title_fragments},
        "集計開始日": {"date": {"start": since_tag}},
        "集計終了日": {"date": {"start": until_tag}},
        "貢献度スコア": {"number": round(score, 2)},
        "投稿数": {"number": posts},
        "ポジティブリアクション数": {"number": reactions},
        "有用な回答数": {"number": answers},
        "ポジティブFB数": {"number": positive_fb},
        "ガイドライン違反数": {"number": violations},
        "期間": {"select": {"name": period}}
    }

    if existing:
        notion.pages.update(page_id=existing[0]["id"], properties=props)
    else:
        notion.pages.create(parent={"database_id": NOTION_DB_ID}, properties=props)


def publish_today_only():
    """
    Fetch and upsert only today's top scores into Notion (period = '本日').
    This can be called periodically (e.g., every hour) from app.py.
    """
    now = datetime.now()
    # Calculate today's start at 00:00
    start_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    since_str = start_today.strftime("%Y-%m-%d")
    until_str = since_str
    # Clear existing 本日 entries
    filter_today = {
        "and": [
            {"property": "期間", "select": {"equals": "本日"}},
            {"property": "集計開始日", "date": {"equals": since_str}},
            {"property": "集計終了日", "date": {"equals": until_str}},
        ]
    }
    cur = notion.databases.query(database_id=NOTION_DB_ID, filter=filter_today)
    for page in cur.get("results", []):
        notion.pages.update(page_id=page["id"], archived=True)
    # Fetch scores from 00:00 today until now
    rows = fetch_user_counts(DB_PATH, start_today.timestamp(), now.timestamp(), limit=TOP_N)
    # Upsert each row, with a trophy for the first place
    for idx, (user_id, posts, reactions, answers, positive_fb, violations, score) in enumerate(rows, start=1):
        mark = '🏅' if idx == 1 else ''
        upsert_to_notion(
            user_id,
            since_str, until_str,
            posts, reactions, answers, positive_fb, violations,
            score,
            period="本日",
            prefix=mark
        )
    logger.info("✅ 本日のランキングを Notion DB に upsert しました。")
    logger.info("publish_today_only: updating timestamp block")
    update_timestamp_block()


def publish_all_periods():
    """
    This can be called periodically (e.g., every day 0:00) from app.py.
    """
    # まず全件クリア
    clear_all_records()

    now = datetime.now()
    # 昨日
    start_yesterday = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_yesterday = start_yesterday + timedelta(days=1)
    rows = fetch_user_counts(DB_PATH, start_yesterday.timestamp(), end_yesterday.timestamp(), limit=TOP_N)
    yesterday_str = start_yesterday.strftime("%Y-%m-%d")
    for idx, (user_id, posts, reactions, answers, positive_fb, violations, score) in enumerate(rows, start=1):
        mark = '🏅' if idx == 1 else ''
        upsert_to_notion(user_id,
                         yesterday_str, yesterday_str,
                         posts, reactions, answers, positive_fb, violations,
                         score, "昨日", prefix=mark)
    logger.info("✅ 昨日のランキングを Notion DB に upsert しました。")

    # 先週 (過去7日)
    start_week = now - timedelta(days=7)
    rows = fetch_user_counts(DB_PATH, start_week.timestamp(), end_yesterday.timestamp(), limit=TOP_N)
    week_since = start_week.strftime("%Y-%m-%d")
    week_until = yesterday_str
    for idx, (user_id, posts, reactions, answers, positive_fb, violations, score) in enumerate(rows, start=1):
        mark = '🏅' if idx == 1 else ''
        upsert_to_notion(user_id,
                         week_since, week_until,
                         posts, reactions, answers, positive_fb, violations,
                         score, "週間", prefix=mark)
    logger.info("✅ 週間ランキングを Notion DB に upsert しました。")

    # 先月 (過去30日)
    start_month = now - timedelta(days=30)
    rows = fetch_user_counts(DB_PATH, start_month.timestamp(), end_yesterday.timestamp(), limit=TOP_N)
    month_since = start_month.strftime("%Y-%m-%d")
    month_until = yesterday_str
    for idx, (user_id, posts, reactions, answers, positive_fb, violations, score) in enumerate(rows, start=1):
        mark = '🏅' if idx == 1 else ''
        upsert_to_notion(user_id,
                         month_since, month_until,
                         posts, reactions, answers, positive_fb, violations,
                         score, "月間", prefix=mark)
    logger.info("✅ 月間ランキングを Notion DB に upsert しました。")

    # 全期間: use earliest event timestamp as since
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT MIN(ts_epoch) FROM events")
    first_ts = cur.fetchone()[0] or 0
    conn.close()
    since_str = datetime.fromtimestamp(first_ts).strftime("%Y-%m-%d")
    until_str = yesterday_str
    rows = fetch_user_counts(DB_PATH, first_ts, end_yesterday.timestamp(), limit=TOP_N)
    for idx, (user_id, posts, reactions, answers, positive_fb, violations, score) in enumerate(rows, start=1):
        mark = '🏅' if idx == 1 else ''
        upsert_to_notion(user_id,
                         since_str, until_str,
                         posts, reactions, answers, positive_fb, violations,
                         score, "全期間", prefix=mark)
    logger.info("✅ 全期間のランキングを Notion DB に upsert しました。")
    logger.info("publish_all_periods: updating timestamp block")
    update_timestamp_block()


if __name__ == "__main__":
    #clear_all_records()
    publish_today_only()
    #publish_all_periods()
