import os
import logging
import sqlite3
from datetime import datetime, timedelta

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from slack_sdk.errors import SlackApiError

from utils.classifier import classify_text
from utils.db import update_score

# ─── ログ設定 ─────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── 環境変数読み込み ───────────────────────────────────
load_dotenv()
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN")
ADMIN_CHANNEL   = os.getenv("ADMIN_CHANNEL")  # 通知先チャンネルID

if not SLACK_BOT_TOKEN or not SLACK_APP_TOKEN or not ADMIN_CHANNEL:
    logger.error("SLACK_BOT_TOKEN, SLACK_APP_TOKEN, or ADMIN_CHANNEL is not set.")
    exit(1)

# ─── Bolt アプリ初期化 ─────────────────────────────────
app = App(token=SLACK_BOT_TOKEN)

# ─── 通知関数 ─────────────────────────────────────────
def notify_violation(user: str, text: str, channel: str, ts: str):
    client = app.client
    try:
        info = client.users_info(user=user)
        profile = info["user"]["profile"]
        username = profile.get("display_name") or profile.get("real_name")
    except SlackApiError:
        username = user
    try:
        link = client.chat_getPermalink(channel=channel, message_ts=ts)["permalink"]
    except SlackApiError:
        link = None
    alert = "<!channel> :rotating_light: *違反検知!*\n"
    alert += f"User: {username}\n"
    alert += f"Text: {text}"
    if link:
        alert += f"\nLink: {link}"
    client.chat_postMessage(channel=ADMIN_CHANNEL, text=alert)

# ─── メッセージ監視ハンドラ（新規／編集を統合） ───────────────────
@app.event("message")
def handle_message_events(event, client):
    # Botやアプリの投稿を除外
    if event.get("bot_id"):
        return

    subtype = event.get("subtype")
    channel = event.get("channel")
    # 管理チャネルは集計・通知対象外
    if channel == ADMIN_CHANNEL:
        return

    # 編集されたメッセージ (subtype=message_changed)
    if subtype == "message_changed":
        message = event.get("message", {})
        text    = message.get("text", "")
        user    = message.get("user")
        ts      = message.get("ts")
        result  = classify_text(text)
        logger.info(f"edited classify_text('{text}') -> {result}")
        if result["violation"]:
            notify_violation(user, text, channel, ts)
            # 既存投稿はカウント済みなので違反のみ追加
            update_score(user, positive=False, violation=True)
        return

    # その他のサブタイプは無視
    if subtype:
        return

    # 新規メッセージ
    user = event.get("user")
    text = event.get("text", "")
    ts   = event.get("ts")

    result = classify_text(text)
    logger.info(f"message classify_text('{text}') -> {result}")

    if result["violation"]:
        notify_violation(user, text, channel, ts)
    update_score(user, positive=result["positive"], violation=result["violation"])

# ─── メンション応答ハンドラ（任意） ────────────────────
@app.event("app_mention")
def handle_mention(event, say):
    user   = event.get("user")
    text   = event.get("text", "")
    result = classify_text(text)
    update_score(user, positive=result["positive"], violation=result["violation"])
    if result["violation"]:
        say("⚠️ ガイドライン違反の可能性です。運営に通知しました。")
    elif result["positive"]:
        say("👍 ポジティブ投稿としてカウントしました！")
    else:
        say("了解しました。投稿を記録しました。")

# ─── /scoreboard コマンドハンドラ ──────────────────────
@app.command("/scoreboard")
def show_scoreboard(ack, respond):
    ack()
    conn = sqlite3.connect("scores.db")
    cur  = conn.cursor()
    cur.execute(
        """
        SELECT user_id, post_count, positive_count, violation_count
        FROM user_scores
        ORDER BY positive_count DESC, post_count DESC
        LIMIT 5
        """
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        respond("まだスコアデータがありません。")
        return

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "🏆 トップ5貢献者", "emoji": True}}
    ]
    for i, (uid, posts, pos, vio) in enumerate(rows):
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": (
                f"*{i+1}. <@{uid}>*\n"
                f"• 投稿数: {posts}\n"
                f"• 👍 ポジティブ: {pos}\n"
                f"• ⚠️ 違反: {vio}"
            )}
        })
    respond(blocks=blocks)

# ─── 定期ランキング投稿関数 ─────────────────────────────
def post_periodic_scoreboard(period_name: str, since: datetime):
    conn = sqlite3.connect("scores.db")
    cur  = conn.cursor()
    cur.execute(
        """
        SELECT user_id,
          SUM(CASE WHEN type='post'      THEN 1 ELSE 0 END) AS post_count,
          SUM(CASE WHEN type='positive'  THEN 1 ELSE 0 END) AS positive_count,
          SUM(CASE WHEN type='violation' THEN 1 ELSE 0 END) AS violation_count
        FROM events
        WHERE ts >= ?
        GROUP BY user_id
        ORDER BY positive_count DESC, post_count DESC
        LIMIT 5
        """,
        (since,)
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        app.client.chat_postMessage(channel=ADMIN_CHANNEL, text=f"{period_name}ランキングデータがありません。")
        return

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"🏅 {period_name}貢献者ランキング", "emoji": True}}
    ]
    for i, (uid, posts, pos, vio) in enumerate(rows):
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": (
            f"*{i+1}. <@{uid}>*\n"
            f"• 投稿数: {posts}\n"
            f"• 👍 ポジティブ: {pos}\n"
            f"• ⚠️ 違反: {vio}"
        )}})
    app.client.chat_postMessage(channel=ADMIN_CHANNEL, blocks=blocks)

# ─── APScheduler 設定 ─────────────────────────────────
scheduler = BackgroundScheduler()
scheduler.add_job(
    lambda: post_periodic_scoreboard("日次", datetime.now() - timedelta(days=1)),
    "cron", hour=0, minute=0
)
scheduler.add_job(
    lambda: post_periodic_scoreboard("週次", datetime.now() - timedelta(days=7)),
    "cron", day_of_week="mon", hour=9, minute=0
)
scheduler.add_job(
    lambda: post_periodic_scoreboard("月次", datetime.now().replace(day=1) - timedelta(days=1)),
    "cron", day=1, hour=9, minute=0
)
scheduler.add_job(
    lambda: post_periodic_scoreboard("四半期", datetime.now().replace(month=((datetime.now().month-1)//3*3+1), day=1) - timedelta(days=1)),
    "cron", month="1,4,7,10", day=1, hour=9, minute=0
)
scheduler.add_job(
    lambda: post_periodic_scoreboard("半期", datetime.now().replace(month=1 if datetime.now().month<7 else 7, day=1) - timedelta(days=1)),
    "cron", month="1,7", day=1, hour=9, minute=0
)
scheduler.add_job(
    lambda: post_periodic_scoreboard("年次", datetime.now().replace(month=1, day=1) - timedelta(days=1)),
    "cron", month="1", day=1, hour=9, minute=0
)
scheduler.start()

# ─── エントリポイント ─────────────────────────────────
if __name__ == "__main__":
    import db_init  # テーブルを初期化
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    logger.info("⚡️ Bolt app is running with scheduler!")
    handler.start()
