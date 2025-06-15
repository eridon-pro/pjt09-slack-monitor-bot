import os
import re
import sqlite3
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from slack_sdk.errors import SlackApiError

import openai
from utils.classifier import classify_text, detect_positive_feedback, is_likely_answer, POSITIVE_REACTIONS, RULES_MAP
import utils.classifier as clf
from utils.db import update_score, record_event, is_positive_reaction, cache_positive_reaction
from utils.llm_judge import judge_positive_reaction, apply_all_positive_reactions

# ─── 環境変数読み込み ───────────────────────────────────
load_dotenv()
SLACK_BOT_TOKEN  = os.getenv("SLACK_BOT_TOKEN")
SLACK_APP_TOKEN  = os.getenv("SLACK_APP_TOKEN")
ADMIN_CHANNEL    = os.getenv("ADMIN_CHANNEL")
QUESTION_CHANNEL = os.getenv("QUESTION_CHANNEL")
openai.api_key   = os.getenv("OPENAI_API_KEY")
clf.MODEL        = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")

if not all([SLACK_BOT_TOKEN, SLACK_APP_TOKEN, ADMIN_CHANNEL, QUESTION_CHANNEL]):
    logger.error("必要な環境変数が設定されていません。")
    exit(1)

# ─── Bolt アプリ初期化 ─────────────────────────────────
app = App(token=SLACK_BOT_TOKEN)

# ─── スコア重み付け設定 ─────────────────────────────────
WEIGHTS = {
    'post': 1.0,
    'reaction': 0.5,
    'answer': 3.0,
    'positive_feedback': 3.0,
    'violation': -5.0,
}

# ─── キャッシュ用辞書 ─────────────────────────────────
USER_CACHE = {}
CHANNEL_CACHE = {}

# ─── 名前解決ヘルパー ─────────────────────────────────
def resolve_user(user_id: str) -> str:
    if user_id in USER_CACHE:
        return USER_CACHE[user_id]
    try:
        res = app.client.users_info(user=user_id)
        profile = res['user']['profile']
        name = profile.get('display_name') or res['user']['name']
    except SlackApiError:
        name = user_id
    USER_CACHE[user_id] = name
    return name

def resolve_channel(channel_id: str) -> str:
    if channel_id in CHANNEL_CACHE:
        return CHANNEL_CACHE[channel_id]
    try:
        res = app.client.conversations_info(channel=channel_id)
        name = res['channel']['name']
    except SlackApiError:
        name = channel_id
    CHANNEL_CACHE[channel_id] = name
    return name

# ─── 複数メンションを名前に変換 ─────────────────────────
def humanize_mentions(text: str) -> str:
    def repl(match):
        uid = match.group(1)
        try:
            uname = resolve_user(uid)
        except SlackApiError:
            uname = uid
        return f"@{uname}"
    return re.sub(r"<@([UW][A-Z0-9]+)>", repl, text)

# ─── ガイドライン違反通知関数 ────────────────────────────────────────
#def notify_violation(user: str, text: str, channel: str, ts: str):
def notify_violation(user: str, text: str, channel: str, ts: str, rules: list[int] | None = None):
    username = resolve_user(user)
    try:
        link = app.client.chat_getPermalink(channel=channel, message_ts=ts)['permalink']
    except SlackApiError:
        link = None
    alert = f"<!channel> :rotating_light: *ガイドライン違反検知!*\n"
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
    app.client.chat_postMessage(channel=ADMIN_CHANNEL, text=alert)
    chan_name = resolve_channel(channel)
    #logger.info(f"notification sent: violation user=@{username} channel=#{chan_name} ts={ts}")
    # ルール番号リストを文字列化
    rules_str = ",".join(str(n) for n in rules) if rules else "None"
    logger.info(
        f"notification sent: violation user=@{username} channel=#{chan_name} ts={ts} "
        f"rules=[{rules_str}]"
    )

# ─── ランキング用ブロック生成 ─────────────────────────────────
def build_scoreboard_blocks(period_name: str, since: datetime = None, until: datetime = None):
    conn = sqlite3.connect('scores.db')
    cur  = conn.cursor()
    if since and until:  # 期間指定の場合
        cur.execute(
            """
            SELECT user_id,
              SUM(CASE WHEN type='post'              THEN 1 ELSE 0 END) as posts,
              SUM(CASE WHEN type='reaction'          THEN 1 ELSE 0 END) as reactions,
              SUM(CASE WHEN type='answer'            THEN 1 ELSE 0 END) as answers,
              SUM(CASE WHEN type='positive_feedback' THEN 1 ELSE 0 END) as pf,
              SUM(CASE WHEN type='violation'         THEN 1 ELSE 0 END) as vio
            FROM events
            WHERE ts_epoch >= ? AND ts_epoch < ?
            GROUP BY user_id
            ORDER BY (?*posts + ?*reactions + ?*answers + ?*pf + ?*vio) DESC
            LIMIT 5
            """, (
                since.timestamp(), until.timestamp(),  # 必要に応じてint化
                WEIGHTS['post'], WEIGHTS['reaction'], WEIGHTS['answer'],
                WEIGHTS['positive_feedback'], WEIGHTS['violation'],
            )
        )
    elif since:
        cur.execute(
            """
            SELECT user_id,
              SUM(CASE WHEN type='post'              THEN 1 ELSE 0 END) as posts,
              SUM(CASE WHEN type='reaction'          THEN 1 ELSE 0 END) as reactions,
              SUM(CASE WHEN type='answer'            THEN 1 ELSE 0 END) as answers,
              SUM(CASE WHEN type='positive_feedback' THEN 1 ELSE 0 END) as pf,
              SUM(CASE WHEN type='violation'         THEN 1 ELSE 0 END) as vio
            FROM events
            WHERE ts_epoch >= ?
            GROUP BY user_id
            ORDER BY (?*posts + ?*reactions + ?*answers + ?*pf + ?*vio) DESC
            LIMIT 5
            """, (
                since.timestamp(),
                WEIGHTS['post'], WEIGHTS['reaction'], WEIGHTS['answer'],
                WEIGHTS['positive_feedback'], WEIGHTS['violation'],
            )
        )
    else:
        cur.execute(
            """
            SELECT user_id,
               post_count as posts,
               reaction_count as reactions,
               answer_count as answers,
               positive_feedback_count as pf,
               violation_count as vio
            FROM user_scores
            ORDER BY (?*posts + ?*reactions + ?*answers + ?*pf + ?*vio) DESC
            LIMIT 5
            """, (
                WEIGHTS['post'], WEIGHTS['reaction'], WEIGHTS['answer'],
                WEIGHTS['positive_feedback'], WEIGHTS['violation'],
            )
        )
    rows = cur.fetchall()
    conn.close()

    if period_name != '累計' and since:
        start = since.strftime('%Y/%m/%d %H:%M')
        # 期間指定なら until、rolling windowなら now
        if until:
            end = (until - timedelta(seconds=1)).strftime('%Y/%m/%d %H:%M')  # 終了日の「23:59」表現にする
        else:
            end = datetime.now().strftime('%Y/%m/%d %H:%M')
        header = f"🏅 {period_name}貢献度ランキング ({start}〜{end})"
    else:
        header = '⏱️ 貢献度ランキング(累計)'

    blocks = [{"type": "header", "text": {"type": "plain_text", "text": header}}]
    if not rows:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "該当ユーザーがいません。"}})
        return blocks

    for i, (uid, posts, reacts, answers, pf, vio) in enumerate(rows, start=1):
        score = (
            WEIGHTS['post'] * posts +
            WEIGHTS['reaction'] * reacts +
            WEIGHTS['answer'] * answers +
            WEIGHTS['positive_feedback'] * pf +
            WEIGHTS['violation'] * vio
        )
        uname = resolve_user(uid)
        text = (
            f"*{i}.* @{uname}  *Score: {score:.1f}*\n"
            f" • 投稿数: {posts}\n"
            f" • 獲得リアクション: {reacts}\n"
            f" • 回答数: {answers}\n"
            f" • 獲得ポジティブFB: {pf}\n"
            f" • ガイドライン違反: {vio}"
        )
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
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
        logger.info(f"message classify_text in #{cname} by @{uname} (ts={ts}): '{text}' -> {result}")
        record_event(user_id, "violation", ts_epoch=ts)
        notify_violation(user_id, text, chan_id, ts, rules)
        update_score(user_id, violation=True)
        #logger.info(f"score updated: user=@{uname} field=violation channel=#{cname} ts={ts}")
        logger.info(
            f"score updated: user=@{uname} field=violation channel=#{cname} ts={ts}"
            #+ (f" rules={result.get('rules')}" if result.get('rules') else "")
            + (f" rules={rules}" if rules else "")
        )
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
        logger.info(
            f"message classify_text in #{cname} by @{uname} (ts={ts}) "
            f"(thread reply): '{text}' -> {result}"
        )
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
    record_event(
        user_id=author_id,
        event_type="reaction",
        ts_epoch=ts_epoch,
        reactor_id=reactor_id,
        reaction_name=reaction
    )

    # 2. POSITIVE_REACTIONS またはポジティブとしてキャッシュ済みにマッチしたときのみ加点（マッチしなかったものは日次でLLMよる判定と判定結果に基づいた加点を行う）
    if reaction in POSITIVE_REACTIONS:
        update_score(author_id, reaction=True)
        logger.info(
            f"reaction_added: {reactor_name} reacted '{reaction}' to "
            f"{author_name}'s message in #{chan_name} (ts={ts}); counted as reaction (STATIC POSITIVE)"
        )
    elif is_positive_reaction(reaction):
        update_score(author_id, reaction=True)
        logger.info(
            f"reaction_added: {reactor_name} reacted '{reaction}' to "
            f"{author_name}'s message in #{chan_name} (ts={ts}); counted as reaction (CACHED POSITIVE)"
        )        
    else:  # ポジティブ未定義のreactionがきたらログに出すだけ。（即時LLM判定する場合はelse以下は不要）
        logger.info(
            f"reaction_added: {reactor_name} reacted '{reaction}' to "
            f"{author_name}'s message in #{chan_name} (ts={ts}); recorded only (will be judged in batch)"
        )
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
        logger.info(f"/apply_reactions executed by {user_id}")
    except Exception as e:
        respond(f"エラー: {e}")
        logger.error(f"/apply_reactions failed: {e}")

# ─── 定期ジョブ設定 ─────────────────────────────────
def post_periodic(period_name, since):
    blocks = build_scoreboard_blocks(period_name, since)
    app.client.chat_postMessage(channel=ADMIN_CHANNEL, blocks=blocks)
    logger.info(f"periodic post: period={period_name}")

scheduler = BackgroundScheduler()
scheduler.add_job(lambda: post_periodic('日次', datetime.now() - timedelta(days=1)), 'cron', hour=0, minute=0)
scheduler.add_job(lambda: post_periodic('週間', datetime.now() - timedelta(days=7)), 'cron', day_of_week='mon', hour=9, minute=0)
scheduler.add_job(lambda: post_periodic('月間', datetime.now() - relativedelta(months=1)), 'cron', day=1, hour=9, minute=0)
scheduler.add_job(lambda: post_periodic('四半期', datetime.now() - relativedelta(months=3)), 'cron', month='1,4,7,10', day=1, hour=9, minute=0)
scheduler.add_job(lambda: post_periodic('半期', datetime.now() - relativedelta(months=6)), 'cron', month='1,7', day=1, hour=9, minute=0)
scheduler.add_job(lambda: post_periodic('年間', datetime.now() - relativedelta(years=1)), 'cron', month='1', day=1, hour=9, minute=0)
scheduler.add_job(apply_all_positive_reactions, 'cron', hour=0, minute=10)
scheduler.start()

if __name__ == '__main__':
    #import db_init
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    handler.start()
