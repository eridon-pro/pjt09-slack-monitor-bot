from dotenv import load_dotenv
load_dotenv()

import os
import sqlite3
from datetime import datetime, timedelta

from notion_client import Client

from utils.constants import WEIGHTS
from utils.slack_helpers import resolve_user

# 環境変数
NOTION_TOKEN   = os.getenv("NOTION_TOKEN")
NOTION_PAGE_ID = os.getenv("NOTION_PAGE_ID")
DB_PATH        = os.getenv("SCORES_DB_PATH", "scores.db")
SLACK_WORKSPACE_URL = os.getenv("SLACK_WORKSPACE_URL", "erikawacom.slack.com")

# Notion クライアント初期化
notion = Client(auth=NOTION_TOKEN)


def replace_page_blocks(page_id: str, blocks: list):
    """
    ページ本文の全ブロックを削除し、渡された blocks リストで置き換える
    """
    # 既存の子ブロックを取得して削除
    children = notion.blocks.children.list(block_id=page_id).get("results", [])
    for block in children:
        notion.blocks.delete(block_id=block["id"])

    # 新しいブロックを一括追加
    notion.blocks.children.append(
        block_id=page_id,
        children=blocks
    )


def fetch_top5(since_ts: float = None, until_ts: float = None, limit: int = 5):
    """
    イベント期間を指定して上位 limit 件の (user_id, score) を返す。
    since_ts, until_ts が None のときは user_scores テーブルの累計値を返す。
    """
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    if since_ts is not None and until_ts is not None:
        # 期間指定：events テーブルから集計
        cur.execute(f"""
            SELECT
              user_id,
              SUM(
                CASE WHEN type='post' THEN 1 ELSE 0 END * ?
                + CASE WHEN type='reaction' AND scored=1 THEN 1 ELSE 0 END * ?
                + CASE WHEN type='answer' THEN 1 ELSE 0 END * ?
                + CASE WHEN type='positive_feedback' THEN 1 ELSE 0 END * ?
                + CASE WHEN type='violation' THEN 1 ELSE 0 END * ?
              ) AS score
            FROM events
            WHERE ts_epoch >= ? AND ts_epoch < ?
            GROUP BY user_id
            ORDER BY score DESC
            LIMIT ?
        """, (
            WEIGHTS['post'],
            WEIGHTS['reaction'],
            WEIGHTS['answer'],
            WEIGHTS['positive_feedback'],
            WEIGHTS['violation'],
            since_ts,
            until_ts,
            limit
        ))
    else:
        # 累計：user_scores テーブルから
        cur.execute(f"""
            SELECT
              user_id,
              (?*post_count + ?*reaction_count + ?*answer_count
               + ?*positive_feedback_count + ?*violation_count) AS score
            FROM user_scores
            ORDER BY score DESC
            LIMIT ?
        """, (
            WEIGHTS['post'],
            WEIGHTS['reaction'],
            WEIGHTS['answer'],
            WEIGHTS['positive_feedback'],
            WEIGHTS['violation'],
            limit
        ))

    rows = cur.fetchall()
    conn.close()
    return rows  # [(user_id, score), ...]


def make_section(title: str, since_dt: datetime, until_dt: datetime):
    """
    タイトルと期間をもとに、Heading2＋番号付きリストのブロックリストを返す
    """
    since_ts = since_dt.timestamp()
    until_ts = until_dt.timestamp()
    top5 = fetch_top5(since_ts, until_ts)

    blocks = [
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [
                    {"type": "text", "text": {"content": title}}
                ]
            }
        }
    ]

    for i, (uid, score) in enumerate(top5, start=1):
        name = resolve_user(uid)
        profile_url = f"{SLACK_WORKSPACE_URL}/team/{uid}" if SLACK_WORKSPACE_URL else None
        # username with @ and optional link
        link_obj = {"link": {"url": profile_url}} if profile_url else {}
        text_segments = [
            {
                "type": "text",
                "text": {"content": f"@{name}", **link_obj},
                "annotations": {"color": "blue"}
            },
            {
                "type": "text",
                "text": {"content": f" — {score:.1f}"}
            }
        ]
        blocks.append({
            "object": "block",
            "type": "numbered_list_item",
            "numbered_list_item": {"rich_text": text_segments}
        })

    # セクション間の区切り（リスト番号リセットのため、divider の後に空段落を入れる）
    blocks.append({
        "object": "block",
        "type": "divider",
        "divider": {}
    })
    blocks.append({
        "object": "block",
        "type": "paragraph",
        "paragraph": {"rich_text": []}
    })

    return blocks


def fetch_top_metric(column: str, limit: int = 5):
    """
    user_scores テーブルから指定したカラムで上位 limit 件を返す。
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(f"""
        SELECT user_id, {column}
        FROM user_scores
        ORDER BY {column} DESC
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    conn.close()
    # Convert to same format (user_id, score)
    return [(uid, float(count)) for uid, count in rows]


def publish_to_notion():
    """
    累計／本日／昨日／週間／月間 のランキングを Notion ページに出力する
    """
    now = datetime.now()

    # ヘッダーと最終更新日時
    header_blocks = [
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {"type": "text", "text": {"content": f"最終更新: {now.strftime('%Y/%m/%d %H:%M')}"}} 
                ]
            }
        },
        {
            "object": "block",
            "type": "divider",
            "divider": {}
        }
    ]

    # 期間を定義
    start_today     = now.replace(hour=0, minute=0, second=0)
    start_yesterday = start_today - timedelta(days=1)
    # 週間: 前日から数えて7日間（前日が終点）
    start_week      = start_today - timedelta(days=7)
    # 月間: 前日から数えて30日間（前日が終点）
    start_month     = start_today - timedelta(days=30)

    date_str_today = now.strftime('%Y/%m/%d')
    date_str_yesterday = start_yesterday.strftime('%Y/%m/%d')
    week_str = f"{start_week.strftime('%Y/%m/%d')}〜{start_yesterday.strftime('%Y/%m/%d')}"
    month_str = f"{start_month.strftime('%Y/%m/%d')}〜{start_yesterday.strftime('%Y/%m/%d')}"

    # 各セクションを組み立て
    all_blocks = header_blocks
    all_blocks += make_section(f"🏅 本日のランキング ({date_str_today})", start_today, now)
    all_blocks += make_section(f"🏅 昨日のランキング ({date_str_yesterday})", start_yesterday, start_today)
    all_blocks += make_section(f"🏅 週間ランキング ({week_str})", start_week, start_yesterday)
    all_blocks += make_section(f"🏅 月間ランキング ({month_str})", start_month, start_yesterday)

    # 投稿数ランキング
    rows = fetch_top_metric("post_count")
    all_blocks.extend([
        {
            "object":"block","type":"heading_2",
            "heading_2":{"rich_text":[{"type":"text","text":{"content":"✏️ 投稿数ランキング"}}]}
        }
    ] + [
        {
            "object":"block","type":"numbered_list_item",
            "numbered_list_item":{"rich_text":[
                {
                    "type":"text","text":{"content":f"@{resolve_user(uid)} — {int(score)}"},
                    "annotations":{"color":"blue"}
                }
            ]}
        }
        for uid, score in rows
    ] + [{"object":"block","type":"divider","divider":{}}])
    # リアクション数ランキング
    rows = fetch_top_metric("reaction_count")
    all_blocks.extend([
        {
            "object":"block","type":"heading_2",
            "heading_2":{"rich_text":[{"type":"text","text":{"content":"👍 リアクション数ランキング"}}]}
        }
    ] + [
        {
            "object":"block","type":"numbered_list_item",
            "numbered_list_item":{"rich_text":[
                {
                    "type":"text","text":{"content":f"@{resolve_user(uid)} — {int(score)}"},
                    "annotations":{"color":"blue"}
                }
            ]}
        }
        for uid, score in rows
    ] + [{"object":"block","type":"divider","divider":{}}])
    # ポジティブフィードバック数ランキング
    rows = fetch_top_metric("positive_feedback_count")
    all_blocks.extend([
        {
            "object":"block","type":"heading_2",
            "heading_2":{"rich_text":[{"type":"text","text":{"content":"💖 ポジティブフィードバック数ランキング"}}]}
        }
    ] + [
        {
            "object":"block","type":"numbered_list_item",
            "numbered_list_item":{"rich_text":[
                {
                    "type":"text","text":{"content":f"@{resolve_user(uid)} — {int(score)}"},
                    "annotations":{"color":"blue"}
                }
            ]}
        }
        for uid, score in rows
    ] + [{"object":"block","type":"divider","divider":{}}])
    # 回答数ランキング
    rows = fetch_top_metric("answer_count")
    all_blocks.extend([
        {
            "object":"block","type":"heading_2",
            "heading_2":{"rich_text":[{"type":"text","text":{"content":"💡 回答数ランキング"}}]}
        }
    ] + [
        {
            "object":"block","type":"numbered_list_item",
            "numbered_list_item":{"rich_text":[
                {
                    "type":"text","text":{"content":f"@{resolve_user(uid)} — {int(score)}"},
                    "annotations":{"color":"blue"}
                }
            ]}
        }
        for uid, score in rows
    ] + [{"object":"block","type":"divider","divider":{}}])

    # Notion ページを丸ごと更新
    replace_page_blocks(NOTION_PAGE_ID, all_blocks)
    print("✅ Notion に全期間ランキングを反映しました")


if __name__ == "__main__":
    publish_to_notion()