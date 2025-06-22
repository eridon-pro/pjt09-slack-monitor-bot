# ─── Standard library imports ─────────────
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
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
QUESTION_CHANNEL = os.getenv("QUESTION_CHANNEL")
BOT_DEV_CHANNEL  = os.getenv("BOT_DEV_CHANNEL")
NOTION_SCOREBOARD_URL = os.getenv("NOTION_SCOREBOARD_URL")
NOTION_VIOLATION_URL = os.getenv("NOTION_VIOLATION_URL")

# ─── Database path ─────────────
DB_PATH = os.getenv("SCORES_DB_PATH", "scores.db")
# ─── Scoreboard Top-N ─────────────
TOP_N = int(os.getenv("TOP_N", "5"))

# openai, clf env assignment
import openai
openai.api_key   = os.getenv("OPENAI_API_KEY")
import utils.classifier as clf
clf.MODEL        = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")

# ─── Third-party imports ─────────────
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from apscheduler.schedulers.background import BackgroundScheduler
from slack_sdk.errors import SlackApiError

# ─── Utils imports ─────────────
from utils.constants import WEIGHTS
from utils.slack_helpers import resolve_user, resolve_channel, humanize_mentions
from utils.classifier import classify_text, detect_positive_feedback, is_likely_answer, POSITIVE_REACTIONS, RULES_MAP
from utils.db import update_score, record_event, is_positive_reaction, cache_positive_reaction, mark_reaction_scored
from utils.llm_judge import judge_positive_reaction, apply_all_positive_reactions
from utils.scoring import fetch_user_counts, compute_score
from publish_master_upsert import publish_today_only, publish_all_periods
from utils.scoring import fetch_user_counts
from violation_trends import main as run_violation_trends
from publish_user_metrics import main as publish_user_metrics

if not all([SLACK_BOT_TOKEN, SLACK_APP_TOKEN, ADMIN_CHANNEL, QUESTION_CHANNEL, BOT_DEV_CHANNEL]):
    logger.error("必要な環境変数が設定されていません。")
    exit(1)

# ─── Bolt アプリ初期化 ─────────────────────────────────
app = App(token=SLACK_BOT_TOKEN)

# ─── ガイドライン違反通知関数 ────────────────────────────────────────
#def notify_violation(user: str, text: str, channel: str, ts: str):
def notify_violation(user: str, text: str, channel: str, ts: str, rules: list[int] | None = None):
    username = resolve_user(user)
    try:
        link = app.client.chat_getPermalink(channel=channel, message_ts=ts)['permalink']
    except SlackApiError:
        link = None
    alert = f"<!channel> :rotating_light: *ガイドライン違反検知！*\n"
    alert += f"User: {username}\nText: {text}\n"
    # 違反規約番号＋本文を追加
    if rules:
        nums = ", ".join(str(n) for n in rules)
        alert += f"Violated Rules: {nums}\n"
        for n in rules:
            body = RULES_MAP.get(n)
            if body:
                alert += f"{n}. {body}\n"
    if link:
        alert += f"\nLink: {link}"
    alert += f"\n\n<{NOTION_VIOLATION_URL}|コミュニティ規約違反傾向分析ページ>"
    app.client.chat_postMessage(channel=ADMIN_CHANNEL, text=alert)
    chan_name = resolve_channel(channel)
    #logger.info(f"notification sent: violation user=@{username} channel=#{chan_name} ts={ts}")
    # ルール番号リストを文字列化
    rules_str = ",".join(str(n) for n in rules) if rules else "None"
    logger.info(f"notification sent: violation user=@{username} channel=#{chan_name} ts={ts} rules=[{rules_str}]")

# ─── ランキング用ブロック生成 ─────────────────────────────────
def build_scoreboard_blocks(period_name: str, since: datetime = None, until: datetime = None):
    # ── fetch_user_counts で集計 ───────────────────────────────────
    db_path  = DB_PATH
    since_ts = since.timestamp() if since else 0.0
    until_ts = until.timestamp() if until else time.time()
    # 返り値: [(user_id, posts, reactions, answers, positive_fb, violations, score), ...]
    rows = fetch_user_counts(db_path, since_ts, until_ts, limit=TOP_N)

    if since and until:
        # Explicit date-range specified by user (YYYYMMDD-YYYYMMDD)
        start = since.strftime('%Y/%m/%d %H:%M')
        end = (until - timedelta(seconds=1)).strftime('%Y/%m/%d %H:%M')
        header = f"🏅 貢献度ランキング (期間 {start}〜{end})"
    elif since and not until:
        # Rolling window (日次, 週間, etc.) - show explicit period
        start_str = since.strftime('%Y/%m/%d %H:%M')
        end_str = datetime.now().strftime('%Y/%m/%d %H:%M')
        header = f"🏅 {period_name}貢献度ランキング (期間 {start_str}〜{end_str})"
    else:
        # Cumulative over all time
        # 全期間用に、DBの最古イベント日時と現在時刻を取得
        cur_db = sqlite3.connect(DB_PATH).cursor()
        cur_db.execute("SELECT MIN(ts_epoch) FROM events")
        min_ts = cur_db.fetchone()[0] or 0.0
        cur_db.connection.close()
        start_dt = datetime.fromtimestamp(min_ts)
        end_dt = datetime.now()
        start_str = start_dt.strftime('%Y/%m/%d %H:%M')
        end_str = end_dt.strftime('%Y/%m/%d %H:%M')
        header = f'⏱️ 貢献度ランキング(全期間 {start_str}〜{end_str})'

    blocks = [{"type": "header", "text": {"type": "plain_text", "text": header}}]
    if not rows:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "該当ユーザーがいません。"}})
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"<{NOTION_SCOREBOARD_URL}|コミュニティ貢献度ランキングページ>"
            }
        })
        return blocks

    for i, (uid, posts, reacts, answers, pf, vio, score) in enumerate(rows, start=1):
        uname = resolve_user(uid)
        text = (
            #f"*{i}.* @{uname}  *Score: {score:.1f}*\n"
            f"*{i}.* <@{uid}>  *Score: {score:.1f}*\n"
            f" • 投稿数: {posts}\n"
            f" • 獲得リアクション: {reacts}\n"
            f" • 回答数: {answers}\n"
            f" • 獲得ポジティブFB: {pf}\n"
            f" • ガイドライン違反: {vio}"
        )
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"<{NOTION_SCOREBOARD_URL}|コミュニティ貢献度ランキングページ>"
        }
    })
    return blocks

# ─── 引数パース ─────────────────────────────────────────
def parse_period(text: str):
    now = datetime.now()
    t = (text or '').strip().lower()

    # --- YYYYMMDD-YYYYMMDD形式で期間指定 ---
    m = re.match(r"(\d{8})-(\d{8})", t)
    if m:
        start = datetime.strptime(m.group(1), "%Y%m%d")
        # 終了日を含めたい場合は1日足す
        end   = datetime.strptime(m.group(2), "%Y%m%d") + timedelta(days=1)
        period_name = f"{start.strftime('%Y/%m/%d')}〜{(end-timedelta(days=1)).strftime('%Y/%m/%d')}"
        return period_name, start, end

    if t == 'today':  # 本日の0:00から現在まで
        start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        return '本日の', start, None
    if t == 'daily':
        return '日次', now - timedelta(days=1), None
    if t == 'weekly':
        return '週間', now - timedelta(days=7), None
    if t == 'monthly':
        return '月間', now - relativedelta(months=1), None
    if t == 'quarterly':
        return '四半期', now - relativedelta(months=3), None
    if t == 'semiannual':
        return '半期', now - relativedelta(months=6), None
    if t == 'annual':
        return '年間', now - relativedelta(years=1), None
    return '累計', None, None

# ─── メッセージ監視ハンドラ（新規・編集対応） ─────────────────
@app.event("message")
def handle_message(event, client):
    #logger.info(f"🔍 handle_message event: ts={event.get('ts')}, subtype={event.get('subtype')}, text={event.get('text')}")  # debug
    if event.get('bot_id'): return
    subtype = event.get('subtype')
    # 編集イベントの場合
    if subtype == 'message_changed':
        ev       = event.get('message', {})
        raw_text = ev.get('text','')
        user_id  = ev.get('user')
        chan_id  = event.get('channel')
        ts       = ev.get('message', {}).get('ts')
    else:
        raw_text = event.get('text','')
        user_id  = event.get('user')
        chan_id  = event.get('channel')
        ts       = event.get('ts')

    if chan_id == ADMIN_CHANNEL:
        return

    text  = humanize_mentions(raw_text)
    cname = resolve_channel(chan_id)
    uname = resolve_user(user_id)

    # 1. ガイドライン違反検知
    result = classify_text(raw_text)
    if result.get('violation'):
        rules = result.get('rules', [])
        # ルールIDリストをカンマ区切り文字列に
        rules_str = ",".join(str(n) for n in rules) if rules else None
        logger.info(f"message classify_text in #{cname} by @{uname} (ts={ts}): '{text}' -> {result}")
        record_event(user_id=user_id, event_type="violation", ts_epoch=ts, violation_rule=rules_str)
        notify_violation(user_id, text, chan_id, ts, rules)
        update_score(user_id, violation=True)
        #logger.info(f"score updated: user=@{uname} field=violation channel=#{cname} ts={ts}")
        logger.info(f"score updated: user=@{uname} field=violation channel=#{cname} ts={ts}{f' rules={rules}' if rules else ''}")
        return

    # 2. ポジティブフィードバック検出
    targets = detect_positive_feedback(raw_text)
    if targets:
        name_list = [resolve_user(uid) for uid in set(targets)]
        logger.info(f"positive feedback detected in #{cname}: targets={name_list} text='{text}'")
        for tgt in set(targets):
            if tgt != user_id:
                record_event(tgt, "positive_feedback", ts_epoch=ts)
                update_score(tgt, positive_feedback=True)
                tname = resolve_user(tgt)
                logger.info(f"score updated: user=@{tname} field=positive_feedback channel=#{cname} ts={ts}")
        return

    # 3. スレッド返信の回答判定（親投稿の作者と同じなら自己返信としてpost扱い）
    if chan_id == QUESTION_CHANNEL and event.get('thread_ts') and event['thread_ts'] != ts:
        logger.info(f"message classify_text in #{cname} by @{uname} (ts={ts}) (thread reply): '{text}' -> {result}")
        parent_ts = event['thread_ts']
        try:
            parent = app.client.conversations_replies(
                channel=chan_id, ts=parent_ts, limit=1
            )['messages'][0]
            parent_user = parent.get('user')
        except SlackApiError:
            parent_user = None

        logger.info(f"thread reply in #{cname} by @{uname} (ts={ts}), parent_author={parent_user}")
        # 自己返信ならpost、それ以外は回答 or post
        #if user_id != parent_user and is_likely_answer(raw_text):
        if user_id != parent_user and is_likely_answer(parent.get('text',''), raw_text):
            record_event(user_id, "answer", ts_epoch=ts)
            update_score(user_id, answer=True)
            logger.info(f"score updated: user=@{uname} field=answer channel=#{cname} ts={ts}")
        else:
            record_event(user_id, "post", ts_epoch=ts)
            update_score(user_id, post=True)
            logger.info(f"score updated: user=@{uname} field=post channel=#{cname} ts={ts}")
        return

    # 4. 通常投稿
    logger.info(f"message classify_text in #{cname} by @{uname} (ts={ts}): '{text}' -> {result}")
    record_event(user_id, "post", ts_epoch=ts)
    update_score(user_id, post=True)
    logger.info(f"score updated: user=@{uname} field=post channel=#{cname} ts={ts}")

# ─── リアクション追加ハンドラ ─────────────────────────
@app.event('reaction_added')
def handle_reaction(event, client):
    #logger.info(f"🔔 raw reaction_added payload: {event}")  ## Debug
    item       = event.get('item', {})
    chan_id    = item.get('channel')
    ts         = item.get('ts')
    reaction   = event.get('reaction')
    reactor_id = event.get('user')
    author_id  = event.get('item_user')  # event.item_user に投稿者 ID があるので、API 呼び出し不要
    ts_epoch   = float(ts) if ts else None

    if not author_id:  # auther_idがない場合は無視
        return

    if reactor_id == author_id:  # セルフリアクションは無視
        return

    author_name  = resolve_user(author_id)
    reactor_name = resolve_user(reactor_id)
    chan_name    = resolve_channel(chan_id)

    #logger.info(f"handle_reaction: author_id={author_id}, reactor_id={reactor_id}, reaction={reaction}, ts_epoch={ts_epoch}")  # Debug

    # 1. すべてのリアクションをDB eventsに記録（加点せず記録のみ！）
    evt_id = record_event(
        user_id=author_id,
        event_type="reaction",
        ts_epoch=ts_epoch,
        reactor_id=reactor_id,
        reaction_name=reaction
    )

    # 2. POSITIVE_REACTIONS またはポジティブとしてキャッシュ済みにマッチしたときのみ加点（マッチしなかったものは日次でLLMよる判定と判定結果に基づいた加点を行う）
    if reaction in POSITIVE_REACTIONS:
        update_score(author_id, reaction=True)
        mark_reaction_scored(evt_id)
        logger.info(f"reaction_added: {reactor_name} reacted '{reaction}' to {author_name}'s message in #{chan_name} (ts={ts}); counted as reaction (STATIC POSITIVE)")
    elif is_positive_reaction(reaction):
        update_score(author_id, reaction=True)
        mark_reaction_scored(evt_id)
        logger.info(f"reaction_added: {reactor_name} reacted '{reaction}' to {author_name}'s message in #{chan_name} (ts={ts}); counted as reaction (CACHED POSITIVE)")
    else:  # ポジティブ未定義のreactionがきたらログに出すだけ。（即時LLM判定する場合はelse以下は不要）
        logger.info(f"reaction_added: {reactor_name} reacted '{reaction}' to {author_name}'s message in #{chan_name} (ts={ts}); recorded only (will be judged in batch)")
    """
    # ここからLLM判定スイッチ
    USE_LLM_REACTION = True  # FalseならPOSITIVE_REACTIONSのみ参照
    elif USE_LLM_REACTION:
        is_pos = is_positive_reaction(reaction)  # キャッシュ参照
        if is_pos is None:  # キャッシュがなければ、judge_positive_reactionでLLM判定し、キャッシュへ
            is_pos = judge_positive_reaction(reaction)
            cache_positive_reaction(reaction, is_pos)
        if is_pos:
            update_score(author_id, reaction=True)
            logger.info(f"reaction_added (LLM): '{reaction}' judge:positive, {reactor_name} reacted '{reaction}' to {author_name}'s message in #{chan_name} (ts={ts}); counted as reaction")
        else:
            logger.info(f"reaction_added (LLM): '{reaction}' judge:not_positive, {reactor_name} reacted '{reaction}' to {author_name}'s message in #{chan_name} (ts={ts}); NOT counted as reaction")
    else:
        # LLM未使用: eventsへの記録のみ
        logger.info(f"reaction_added: {reactor_name} reacted '{reaction}' to {author_name}'s message in #{chan_name} (ts={ts}); no LLM check (no score change)")
    """

# ─── reaction削除ハンドラ ───────────────────────────
@app.event('reaction_removed')
def handle_reaction_removed(event, client):
    # 無視ポリシーなので、何もしない
    pass

# ─── /scoreboard コマンドハンドラ ──────────────────────
@app.command('/scoreboard')
def show_scoreboard(ack, body, respond):
    ack()
    user_chan = body.get('channel_id')
    if user_chan != ADMIN_CHANNEL:
        respond(f"このコマンドは <#{ADMIN_CHANNEL}> でのみ使用できます。")
        return
    #period_name, since = parse_period(body.get('text',''))
    period_name, since, until = parse_period(body.get('text',''))
    #print(f"[DEBUG] since={since} ({since.timestamp() if since else None}) until={until} ({until.timestamp() if until else None})")  ## Debug
    #blocks = build_scoreboard_blocks(period_name, since)
    blocks = build_scoreboard_blocks(period_name, since, until)
    respond(blocks=blocks)
    uname = resolve_user(body['user_id'])
    logger.info(f"/scoreboard executed: period={period_name} user=@{uname}")

# ─── /apply_reactions コマンドハンドラ ──────────────────────
@app.command("/apply_reactions")
def handle_apply_reactions(ack, body, respond, logger):
    ack()
    user_chan = body.get('channel_id')
    if user_chan != ADMIN_CHANNEL:
        respond(f"このコマンドは <#{ADMIN_CHANNEL}> でのみ使用できます。")
        return

    user_id = body.get("user_id")
    try:
        apply_all_positive_reactions()
        respond(f"<@{user_id}> 未知のポジティブリアクションのLLM判定・加点を実行しました。")
        uname = resolve_user(body['user_id'])
        logger.info(f"/apply_reactions executed by user=@{uname}")
    except Exception as e:
        respond(f"エラー: {e}")
        logger.error(f"/apply_reactions failed: {e}")

# ─── 定期ジョブ設定 ─────────────────────────────────
def post_periodic(period_name, since, channel=ADMIN_CHANNEL):
    blocks = build_scoreboard_blocks(period_name, since)
    app.client.chat_postMessage(channel=channel, blocks=blocks)
    logger.info(f"periodic post: period={period_name}")

scheduler = BackgroundScheduler()
scheduler.add_job(lambda: post_periodic('日次', datetime.now() - timedelta(days=1)), 'cron', hour=0, minute=0)
scheduler.add_job(lambda: post_periodic('週間', datetime.now() - timedelta(days=7)), 'cron', day_of_week='mon', hour=9, minute=0)
scheduler.add_job(lambda: post_periodic('月間', datetime.now() - relativedelta(months=1)), 'cron', day=1, hour=9, minute=0)
# 「今月の貢献者紹介」機能
scheduler.add_job(lambda: post_periodic('月間', datetime.now() - relativedelta(months=1), channel=BOT_DEV_CHANNEL), 'cron', day=1, hour=9, minute=0)
scheduler.add_job(lambda: post_periodic('四半期', datetime.now() - relativedelta(months=3)), 'cron', month='1,4,7,10', day=1, hour=9, minute=0)
scheduler.add_job(lambda: post_periodic('半期', datetime.now() - relativedelta(months=6)), 'cron', month='1,7', day=1, hour=9, minute=0)
scheduler.add_job(lambda: post_periodic('年間', datetime.now() - relativedelta(years=1)), 'cron', month='1', day=1, hour=9, minute=0)
scheduler.add_job(apply_all_positive_reactions, 'cron', hour=0, minute=10)

def scheduled_publish_today():
    try:
        publish_today_only()
    except Exception:
        logger.exception("scheduled_publish_today でエラーが発生しました")

#scheduler.add_job(publish_today_only, 'cron', minute=0)
scheduler.add_job(scheduled_publish_today, 'cron', minute=10)
scheduler.add_job(publish_all_periods, 'cron', hour=0, minute=0)
scheduler.add_job(run_violation_trends, 'cron', hour=0, minute=0, id='violation_trends')
scheduler.add_job(publish_user_metrics, 'cron', hour=0, minute=30, id='publish_user_metrics')
scheduler.start()

if __name__ == '__main__':
    #import db_init
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    handler.start()
