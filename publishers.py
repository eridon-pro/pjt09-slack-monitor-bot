import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from notion_client import Client as NotionClient
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Dotenv load ─────────────
from dotenv import load_dotenv

load_dotenv()

# ─── Environment variable reads ─────────────
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN")
ADMIN_CHANNEL = os.getenv("ADMIN_CHANNEL")
# QUESTION_CHANNEL = os.getenv("QUESTION_CHANNEL")
# BOT_DEV_CHANNEL  = os.getenv("BOT_DEV_CHANNEL")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_TREND_PAGE_ID = os.getenv("NOTION_TREND_PAGE_ID")
NOTION_TREND_URL = os.getenv("NOTION_TREND_URL")


# ─── Database path ─────────────
DB_PATH = os.getenv("SCORES_DB_PATH", "scores.db")


def post_faq_to_slack(db, slack_token=SLACK_BOT_TOKEN, channel=ADMIN_CHANNEL):
    client = WebClient(token=slack_token)
    cur = db.cursor()
    cur.execute(
        "SELECT id, title, answer, source_url, created_at FROM extracted_items ORDER BY id"
    )
    rows = cur.fetchall()

    blocks = []
    blocks.append(
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "💡 過去7日間のよくある質問(FAQ)"},
        }
    )
    blocks.append({"type": "divider"})

    # Get the latest update date for display at the end
    latest_ts = max(float(ts) for _, _, _, _, ts in rows) if rows else 0
    latest_dt = datetime.fromtimestamp(latest_ts, timezone(timedelta(hours=9)))
    latest_updated = latest_dt.strftime("%Y-%m-%d %H:%M JST")

    for _, q, a, url, ts in rows:
        text = f"*Q:* {q}\n\n*A:* {a}"
        if url:
            parts = url.split(":", 1)
            if len(parts) == 2:
                label, link = parts[0].strip(), parts[1].strip()
                text += f"\n\nこちらもご参照ください → <{link}|{label}>"
            else:
                text += f"\n\nこちらもご参照ください → {url}"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
        blocks.append({"type": "divider"})

    # Add latest update time before the link
    blocks.append(
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"_最終更新: {latest_updated}_"},
        }
    )
    blocks.append(
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"<{NOTION_TREND_URL}|コミュニティトレンド分析> ページへ",
            },
        }
    )

    if not blocks:
        logger.info("No FAQ entries to post to Slack.")
        return

    client.chat_postMessage(
        channel=channel,
        text="💡 過去7日間のよくある質問(FAQ)をお届けします",
        blocks=blocks,
    )


def post_trends_to_slack(db, slack_token=SLACK_BOT_TOKEN, channel=ADMIN_CHANNEL):
    client = WebClient(token=slack_token)
    cur = db.cursor()
    cur.execute(
        "SELECT label, topic_text, size, created_at FROM trend_topics ORDER BY size DESC, created_at DESC"
    )
    rows = cur.fetchall()

    # Prepare top N and header
    top_n = rows[:5]
    count = len(top_n)
    header_text = f"📈 過去7日間のトレンドトピック{count}位"

    blocks = []
    blocks.append(
        {"type": "header", "text": {"type": "plain_text", "text": header_text}}
    )
    blocks.append({"type": "divider"})

    if not rows:
        logger.info("No trend topics to post to Slack.")
        return

    # Get the latest registration date for display at the end
    latest_ts = max(float(ts) for _, _, _, ts in top_n) if top_n else 0
    latest_dt = datetime.fromtimestamp(latest_ts, timezone(timedelta(hours=9)))
    latest_created_at_str = latest_dt.strftime("%Y-%m-%d %H:%M JST")

    for idx, (_, topic_text, size, ts) in enumerate(top_n):
        if idx == 0:
            text = f":sports_medal: *トピック:* {topic_text}\n*投稿数:* {size}"
        else:
            text = f"*トピック:* {topic_text}\n*投稿数:* {size}"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
        blocks.append({"type": "divider"})

    blocks.append(
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"_最終更新: {latest_created_at_str}_"},
        }
    )

    blocks.append(
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"<{NOTION_TREND_URL}|コミュニティトレンド分析> ページへ",
            },
        }
    )

    client.chat_postMessage(
        channel=channel, text="📈 最新のトレンドトピックをお届けします", blocks=blocks
    )


def post_info_requests_to_slack(db, slack_token=SLACK_BOT_TOKEN, channel=ADMIN_CHANNEL):
    """
    Post recent information requests to Slack.
    """
    client = WebClient(token=slack_token)
    cur = db.cursor()
    # Fetch top 5 info requests by most requested items (size)
    cur.execute(
        "SELECT id, request_text, size, created_at "
        "FROM info_requests "
        "ORDER BY size DESC, created_at DESC "
        "LIMIT 5"
    )
    rows = cur.fetchall()

    # Get the latest update date for display at the end
    latest_ts = max(float(ts) for _, _, _, ts in rows) if rows else 0
    latest_dt = datetime.fromtimestamp(latest_ts, timezone(timedelta(hours=9)))
    latest_updated = latest_dt.strftime("%Y-%m-%d %H:%M JST")

    text = "<!channel> 📣 *受講生から多く求められている情報(過去7日間分)*\n\n"
    for idx, (req_id, req_text, size, ts) in enumerate(rows, start=1):
        try:
            items = json.loads(req_text)
        except Exception:
            items = [req_text]
        text += f"*カテゴリー{idx}* (件数: {size})\n"
        for item in items:
            text += f"- {item}\n"
        text += "\n"
    text += f"_最終更新: {latest_updated}_\n"
    text += f"<{NOTION_TREND_URL}|コミュニティトレンド分析> ページへ\n"

    client.chat_postMessage(channel=channel, text=text)


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
    cur.execute(
        "SELECT title, answer, source_url, created_at FROM extracted_items ORDER BY id"
    )
    rows = cur.fetchall()

    # Build children blocks
    children = [
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": "💡 過去7日間のよくある質問(FAQ)"},
                    }
                ]
            },
        }
    ]
    children.append({"object": "block", "type": "divider", "divider": {}})

    # Get the latest update date for display at the end
    latest_ts = max(float(ts) for _, _, _, ts in rows) if rows else 0
    latest_dt = datetime.fromtimestamp(latest_ts, timezone(timedelta(hours=9)))
    latest_updated = latest_dt.strftime("%Y-%m-%d %H:%M JST")

    for title, answer, url, ts in rows:
        # Replace question heading_3 block with paragraph block with bold markdown
        children.append(
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": f"Q: {title}"},
                            "annotations": {"bold": True},
                        }
                    ]
                },
            }
        )
        # answer paragraph
        children.append(
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": f"A: {answer}"}}]
                },
            }
        )
        if url:
            parts = url.split(":", 1)
            if len(parts) == 2:
                label, link = parts[0].strip(), parts[1].strip()
                children.append(
                    {
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
                                    "text": {"content": label, "link": {"url": link}},
                                },
                            ]
                        },
                    }
                )
            else:
                paragraph = f"こちらもご参照ください → {url}"
                children.append(
                    {
                        "object": "block",
                        "type": "paragraph",
                        "paragraph": {
                            "rich_text": [
                                {"type": "text", "text": {"content": paragraph}}
                            ]
                        },
                    }
                )
        children.append({"object": "block", "type": "divider", "divider": {}})

    # Add latest update time at the end of FAQ section
    children.append(
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {"type": "text", "text": {"content": f"最終更新: {latest_updated}"}}
                ]
            },
        }
    )

    # Do not append batch timestamp here; it will be appended at the end of notion_upsert_all
    notion.blocks.children.append(block_id=page_id, children=children)
    logger.info(f"Updated Notion page {page_id} with FAQ entries.")


def notion_upsert_trends(db, notion, page_id):
    """
    Upsert trend topics section.
    """
    cur = db.cursor()
    cur.execute(
        "SELECT label, topic_text, size, created_at FROM trend_topics ORDER BY size DESC, created_at DESC"
    )
    rows = cur.fetchall()

    top_n = rows[:5]
    count = len(top_n)
    children = []
    # Heading block
    children.append(
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": f"\n📈 過去7日間のトレンドトピック{count}位"
                        },
                    }
                ]
            },
        }
    )
    # Divider
    children.append({"object": "block", "type": "divider", "divider": {}})

    # Get the latest registration date for display at the end
    latest_ts = max(float(ts) for _, _, _, ts in top_n) if top_n else 0
    try:
        latest_dt = datetime.fromtimestamp(latest_ts, timezone(timedelta(hours=9)))
    except Exception:
        latest_dt = datetime.now(timezone(timedelta(hours=9)))
    latest_created = latest_dt.strftime("%Y-%m-%d %H:%M JST")

    for idx, (_, topic_text, size, ts) in enumerate(top_n, 1):
        # Heading_3 block for topic
        content_text = f"🏅 {idx}. {topic_text}" if idx == 1 else f"{idx}. {topic_text}"
        children.append(
            {
                "object": "block",
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [{"type": "text", "text": {"content": content_text}}]
                },
            }
        )
        # Only show post count, remove registration date
        info_text = f"投稿数: {size}"
        children.append(
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": info_text}}]
                },
            }
        )
        # Divider after each topic
        children.append({"object": "block", "type": "divider", "divider": {}})

    # Add latest registration time at the end of trends section
    children.append(
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {"type": "text", "text": {"content": f"最終更新: {latest_created}"}}
                ]
            },
        }
    )

    notion.blocks.children.append(block_id=page_id, children=children)
    logger.info(f"Updated Notion page {page_id} with trend topics.")


def notion_upsert_info_requests(db, notion, page_id):
    """
    Upsert information request section.
    """
    # Fetch info request entries
    cur = db.cursor()
    cur.execute(
        "SELECT id, request_text, size, created_at FROM info_requests ORDER BY size DESC, created_at DESC LIMIT 5"
    )
    rows = cur.fetchall()

    # Build Notion blocks for info requests
    children = []
    # Section heading
    children.append(
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "\n📣 受講生から多く求められている情報(過去7日間分)"
                        },
                    }
                ]
            },
        }
    )
    # Divider
    children.append({"object": "block", "type": "divider", "divider": {}})

    # Get the latest registration date for display at the end
    latest_ts = max(float(ts) for _, _, _, ts in rows) if rows else 0
    try:
        latest_dt = datetime.fromtimestamp(latest_ts, timezone(timedelta(hours=9)))
    except Exception:
        latest_dt = datetime.now(timezone(timedelta(hours=9)))
    latest_created = latest_dt.strftime("%Y-%m-%d %H:%M JST")

    for idx, (_id, req_text, size, ts) in enumerate(rows, start=1):
        # Decode JSON list or single string
        try:
            items = json.loads(req_text)
            if not isinstance(items, list):
                items = [items]
        except Exception:
            items = [req_text]

        # Heading for each category
        title = (
            f"🏅 カテゴリー{idx} (件数: {size})"
            if idx == 1
            else f"カテゴリー{idx} (件数: {size})"
        )
        children.append(
            {
                "object": "block",
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [{"type": "text", "text": {"content": title}}]
                },
            }
        )

        # Add each requested item as a bulleted list
        for item in items:
            children.append(
                {
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {
                        "rich_text": [{"type": "text", "text": {"content": item}}]
                    },
                }
            )

        # Divider after each category
        children.append({"object": "block", "type": "divider", "divider": {}})

    # Add latest registration time at the end of info requests section
    children.append(
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {"type": "text", "text": {"content": f"最終更新: {latest_created}"}}
                ]
            },
        }
    )

    # Append to page
    notion.blocks.children.append(block_id=page_id, children=children)
    logger.info(f"Updated Notion page {page_id} with info requests.")


def append_batch_timestamp(children):
    """
    Append a batch execution timestamp block in JST to the given children list.
    """
    now = datetime.now(timezone(timedelta(hours=9)))
    batch_time = now.strftime("%Y-%m-%d %H:%M JST")
    children.append(
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": f"\n\nバッチ実行日時: {batch_time}"},
                    }
                ]
            },
        }
    )


def notion_upsert_all(db, notion_token=NOTION_TOKEN, page_id=NOTION_TREND_PAGE_ID):
    """
    Clear the Notion page and upsert all sections: FAQs, trends, and info requests.
    """
    if not notion_token or not page_id:
        logger.error(
            "Notion token or page ID not configured; skipping full Notion update."
        )
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
