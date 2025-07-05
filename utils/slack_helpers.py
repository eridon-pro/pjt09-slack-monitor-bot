import os
import re
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Slack Web API クライアントの初期化
slack_client = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))

# キャッシュ用辞書
USER_CACHE = {}
CHANNEL_CACHE = {}


def resolve_user(user_id: str) -> str:
    """
    Slack user_id を人間が読める表示名に変換。
    display_name → real_name → user.name の順で取得し、
    失敗時は user_id をそのまま返す。
    """
    if user_id in USER_CACHE:
        return USER_CACHE[user_id]
    try:
        res = slack_client.users_info(user=user_id)
        profile = res["user"]["profile"]
        name = (
            profile.get("display_name")
            or profile.get("real_name")
            or res["user"]["name"]
        )
    except SlackApiError:
        name = user_id
    USER_CACHE[user_id] = name
    return name


def resolve_channel(channel_id: str) -> str:
    """
    Slack channel_id をチャンネル名（#xxx）に変換。
    失敗時は channel_id をそのまま返す。
    """
    if channel_id in CHANNEL_CACHE:
        return CHANNEL_CACHE[channel_id]
    try:
        res = slack_client.conversations_info(channel=channel_id)
        name = res["channel"]["name"]
    except SlackApiError:
        name = channel_id
    CHANNEL_CACHE[channel_id] = name
    return name


def humanize_mentions(text: str) -> str:
    """
    テキスト中の <@UXXXXXXX> を @display_name に置換。
    複数メンションにも対応。
    """

    def repl(match):
        uid = match.group(1)
        uname = resolve_user(uid)
        return f"@{uname}"

    return re.sub(r"<@([UW][A-Z0-9]+)>", repl, text)
