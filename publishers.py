import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from notion_client import Client as NotionClient
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ─── Dotenv load ─────────────
from dotenv import load_dotenv
load_dotenv()

# ─── Environment variable reads ─────────────
SLACK_BOT_TOKEN  = os.getenv("SLACK_BOT_TOKEN")
SLACK_APP_TOKEN  = os.getenv("SLACK_APP_TOKEN")
ADMIN_CHANNEL    = os.getenv("ADMIN_CHANNEL")
#QUESTION_CHANNEL = os.getenv("QUESTION_CHANNEL")
#BOT_DEV_CHANNEL  = os.getenv("BOT_DEV_CHANNEL")
NOTION_TOKEN     = os.getenv("NOTION_TOKEN")
NOTION_TREND_PAGE_ID   = os.getenv("NOTION_TREND_PAGE_ID")


# ─── Database path ─────────────
DB_PATH = os.getenv("SCORES_DB_PATH", "scores.db")


def post_faq_to_slack(db, slack_token=SLACK_BOT_TOKEN, channel=ADMIN_CHANNEL):
    client = WebClient(token=slack_token)
    cur = db.cursor()
    cur.execute("SELECT id, title, answer, source_url, created_at FROM extracted_items ORDER BY id")
    rows = cur.fetchall()

    blocks = []
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": "💡 過去7日間のよくある質問(FAQ)"}
    })
    blocks.append({"type": "divider"})

    for _, q, a, url, ts in rows:
        # parse created_at as float timestamp or ISO string, then convert to JST
        dt = datetime.fromtimestamp(float(ts), timezone(timedelta(hours=9)))
        updated = dt.strftime('%Y-%m-%d %H:%M')
        text = f"*Q:* {q}\n\n*A:* {a}"
        if url:
            parts = url.split(':', 1)
            if len(parts) == 2:
                label, link = parts[0].strip(), parts[1].strip()
                text += f"\n\nこちらもご参照ください → <{link}|{label}>"
            else:
                text += f"\n\nこちらもご参照ください → {url}"
        text += f"\n\n_最終更新: {updated}_"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
        blocks.append({"type": "divider"})

    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "コミュニティトレンド分析: <https://www.notion.so/21408ac90f4c80a9b60ad7d250967ca9|Notion ページへ>"
        }
    })

    if not blocks:
        logger.info("No FAQ entries to post to Slack.")
        return

    client.chat_postMessage(
        channel=channel,
        text="💡 過去7日間のよくある質問(FAQ)をお届けします",
        blocks=blocks
    )

def post_trends_to_slack(db, slack_token=SLACK_BOT_TOKEN, channel=ADMIN_CHANNEL):
    client = WebClient(token=slack_token)
    cur = db.cursor()
    cur.execute("SELECT label, topic_text, size, created_at FROM trend_topics ORDER BY size DESC, created_at DESC")
    rows = cur.fetchall()

    # Prepare top N and header
    top_n = rows[:5]
    count = len(top_n)
    header_text = f"📈 過去7日間のトレンドトピック{count}位"

    blocks = []
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": header_text}
    })
    blocks.append({"type": "divider"})

    if not rows:
        logger.info("No trend topics to post to Slack.")
        return

    for idx, (_, topic_text, size, ts) in enumerate(top_n):
        # Convert stored UTC timestamp to JST
        dt = datetime.fromtimestamp(float(ts), timezone.utc).astimezone(timezone(timedelta(hours=9)))
        created_at_str = dt.strftime('%Y-%m-%d %H:%M')
        if idx == 0:
            text = f":sports_medal: *トピック:* {topic_text}\n*投稿数:* {size}\n_登録日時: {created_at_str}_"
        else:
            text = f"*トピック:* {topic_text}\n*投稿数:* {size}\n_登録日時: {created_at_str}_"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
        blocks.append({"type": "divider"})

    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "コミュニティトレンド分析: <https://www.notion.so/21408ac90f4c80a9b60ad7d250967ca9|Notion ページへ>"
        }
    })

    client.chat_postMessage(
        channel=channel,
        text="📈 最新のトレンドトピックをお届けします",
        blocks=blocks
    )


def clear_notion_page(notion, page_id):
    """
    Delete all existing child blocks of the given Notion page.
    """
    try:
        # Retrieve all existing child blocks on the page
        children = notion.blocks.children.list(block_id=page_id).get("results", [])
        for child in children:
            notion.blocks.delete(block_id=child["id"])
    except Exception as e:
        logger.warning(f"Could not clear Notion page {page_id}: {e}")


def notion_upsert_faq(db, notion, page_id):
    """
    Upsert FAQ entries to a Notion page. Overwrites the page content with current FAQs.
    """
    # Fetch FAQ entries
    cur = db.cursor()
    cur.execute("SELECT title, answer, source_url, created_at FROM extracted_items ORDER BY id")
    rows = cur.fetchall()

    # Build children blocks
    children = [{
        "object": "block",
        "type": "heading_2",
        "heading_2": {"rich_text": [{"type": "text", "text": {"content": "💡 過去7日間のよくある質問(FAQ)"}}]}
    }]
    children.append({"object": "block", "type": "divider", "divider": {}})

    for title, answer, url, ts in rows:
        # Replace question heading_3 block with paragraph block with bold markdown
        children.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": f"Q: {title}"},
                        "annotations": {"bold": True}
                    }
                ]
            }
        })
        # answer paragraph
        children.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {"rich_text": [{"type": "text", "text": {"content": f"A: {answer}"}}]}
        })
        if url:
            parts = url.split(":", 1)
            if len(parts) == 2:
                label, link = parts[0].strip(), parts[1].strip()
                children.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {"content": "こちらもご参照ください → "},
                            },
                            {
                                "type": "text",
                                "text": {"content": label, "link": {"url": link}}
                            }
                        ]
                    }
                })
            else:
                paragraph = f"こちらもご参照ください → {url}"
                children.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {"rich_text": [{"type": "text", "text": {"content": paragraph}}]}
                })
        # timestamp
        dt = datetime.fromtimestamp(float(ts), timezone(timedelta(hours=9)))
        updated = dt.strftime("%Y-%m-%d %H:%M")
        children.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {"rich_text": [{"type": "text", "text": {"content": f"最終更新: {updated}"}}]}
        })
        children.append({"object": "block", "type": "divider", "divider": {}})

    # Do not append batch timestamp here; it will be appended at the end of notion_upsert_all
    notion.blocks.children.append(block_id=page_id, children=children)
    logger.info(f"Updated Notion page {page_id} with FAQ entries.")


def notion_upsert_trends(db, notion, page_id):
    """
    Upsert trend topics section.
    """
    cur = db.cursor()
    cur.execute("SELECT label, topic_text, size, created_at FROM trend_topics ORDER BY size DESC, created_at DESC")
    rows = cur.fetchall()

    top_n = rows[:5]
    count = len(top_n)
    children = []
    # Heading block
    children.append({
        "object": "block",
        "type": "heading_2",
        "heading_2": {
            "rich_text": [
                {"type": "text", "text": {"content": f"📈 過去7日間のトレンドトピック{count}位"}}
            ]
        }
    })
    # Divider
    children.append({"object": "block", "type": "divider", "divider": {}})

    for idx, (_, topic_text, size, ts) in enumerate(top_n, 1):
        # Heading_3 block for topic
        content_text = f"🏅 {idx}. {topic_text}" if idx == 1 else f"{idx}. {topic_text}"        
        children.append({
            "object": "block",
            "type": "heading_3",
            "heading_3": {
                "rich_text": [
                    #{"type": "text", "text": {"content": f"{idx}. {topic_text}"}}
                    {"type": "text", "text": {"content": content_text}}
                ]
            }
        })
        # Parse created_at as JST
        try:
            dt = datetime.fromtimestamp(float(ts), timezone(timedelta(hours=9)))
        except Exception:
            dt = datetime.now(timezone(timedelta(hours=9)))
        created = dt.strftime('%Y-%m-%d %H:%M')
        info_text = f"投稿数: {size}  |  登録日時: {created}"
        children.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {"type": "text", "text": {"content": info_text}}
                ]
            }
        })
        # Divider after each topic
        children.append({"object": "block", "type": "divider", "divider": {}})

    notion.blocks.children.append(block_id=page_id, children=children)
    logger.info(f"Updated Notion page {page_id} with trend topics.")

def notion_upsert_info_requests(db, notion, page_id):
    """
    Upsert information request section. (To be implemented)
    """
    # TODO: generate and append info request blocks
    pass

def append_batch_timestamp(children):
    """
    Append a batch execution timestamp block in JST to the given children list.
    """
    now = datetime.now(timezone(timedelta(hours=9)))
    batch_time = now.strftime("%Y-%m-%d %H:%M")
    children.append({
        "object": "block",
        "type": "paragraph",
        "paragraph": {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": f"バッチ実行日時: {batch_time}"}
                }
            ]
        }
    })

def notion_upsert_all(db, notion_token=NOTION_TOKEN, page_id=NOTION_TREND_PAGE_ID):
    """
    Clear the Notion page and upsert all sections: FAQs, trends, and info requests.
    """
    if not notion_token or not page_id:
        logger.error("Notion token or page ID not configured; skipping full Notion update.")
        return

    notion = NotionClient(auth=notion_token)

    # 1. Clear existing content
    clear_notion_page(notion, page_id)

    # 2. Prepare and append each section
    notion_upsert_faq(db, notion, page_id)
    notion_upsert_trends(db, notion, page_id)
    notion_upsert_info_requests(db, notion, page_id)

    # 3. Retrieve children for timestamp appending
    children = []
    append_batch_timestamp(children)
    notion.blocks.children.append(block_id=page_id, children=children)

    logger.info(f"Completed full upsert of Notion page {page_id}.")