import os
import openai
from utils.db import get_unjudged_reactions, cache_positive_reaction, get_unscored_positive_reactions, apply_reaction_scores
import logging
logger = logging.getLogger(__name__)

# Default Model overwritten by .env file
#MODEL = "gpt-3.5-turbo"
MODEL = os.environ.get("OPENAI_MODEL", "gpt-3.5-turbo")

def judge_positive_reaction(reaction_name: str) -> bool:
    """
    絵文字リアクション名（例: 'pray', 'clap', 'sparkles'）が
    ポジティブ（感謝・称賛）かどうか LLMで判定する。
    """
    if not openai.api_key:
        logger.info(f"[LLM: {MODEL}] judge_positive_reaction skipped (no API key): {reaction_name}")
        return None  # フォールバックがあれば追記

    system = "あなたはSlackコミュニティの管理AIです。"
    user_prompt = (
        f"Slackのリアクションで':{reaction_name}:'は、他者への感謝や賞賛・肯定的な意味を持つポジティブなものですか？\n"
        "必ず 'yes' か 'no' のどちらか1単語のみで答えてください。理由や補足は一切書かないでください。"
    )
    create_args = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_prompt},
        ],
    }
    # o4-系モデルはtemperature指定不要
    if not MODEL.startswith("o4-"):
        create_args["temperature"] = 0

    try:
        logger.info(f"[LLM: {MODEL}] judge_positive_reaction req: ':{reaction_name}:'")
        resp = openai.chat.completions.create(**create_args)
        content = resp.choices[0].message.content.strip().lower()
        logger.info(f"[LLM: {MODEL}] judge_positive_reaction resp for ':{reaction_name}:': '{content}'")
        return content == "yes"
    except Exception as e:
        logger.warning(f"[LLM: {MODEL}] judge_positive_reaction failed: {e}")
        return None  # フォールバックがあれば追記


def apply_all_positive_reactions():
    """
    キャッシュ内の未判定のリアクションをLLM判定し、ポジティブなら加点まで実施。
    """
    unjudged = get_unjudged_reactions()
    logger.info(f"[LLM-batch: {MODEL}] unjudged reactions = {unjudged}")
    # まず LLM 判定とキャッシュ
    for reaction in unjudged:
        is_pos = judge_positive_reaction(reaction)
        cache_positive_reaction(reaction, is_pos)
        logger.info(f"[LLM-batch: {MODEL}] {reaction}: {'POSITIVE' if is_pos else 'not positive'}")

    # 直近で「判定済みPOSITIVE」になったリアクションのイベントを集める
    events = get_unscored_positive_reactions()
    logger.info(f"[LLM-batch: {MODEL}] found {len(events)} unscored positive reactions")

    # 加点処理
    apply_reaction_scores(events)
    logger.info(f"[LLM-batch: {MODEL}] done: {len(events)} reactions scored")

    if events:
        user_reactions = {}
        for event in events:
            uid = event["user_id"]
            reaction = event["reaction_name"]
            if uid not in user_reactions:
                user_reactions[uid] = []
            user_reactions[uid].append(reaction)
        details = '\n'.join(
            #f'  {resolve_user(uid)}: {", ".join(reactions)}'
            f'  {uid}: {", ".join(reactions)}'
            for uid, reactions in user_reactions.items()
        )
        logger.info(f"[LLM-batch: {MODEL}] Positive reactions judged by LLM:\n{details}")
    else:
        logger.info(f"[LLM-batch: {MODEL}] No users were awarded points in this batch.")
