import os
import logging
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

from dotenv import load_dotenv
import openai
from utils.db import fetch_thread_replies, fetch_post_text
import requests
from bs4 import BeautifulSoup
import json
from sklearn.metrics.pairwise import cosine_similarity

# Load environment variables
load_dotenv()
MODEL = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")
EMBED_MODEL = os.getenv("OPENAI_EMBED_MODEL", "text-embedding-3-small")
KNOWLEDGE_URL1 = os.getenv("KNOWLEDGE_URL1")  # Knowledge site URL
KNOWLEDGE_URL2 = os.getenv(
    "KNOWLEDGE_URL2"
)  # Local FAQ JSONL file path (set via env or default)
FAQ_PATH = os.getenv("FAQ_PATH", "utils/faq_20250628.jsonl")

# Cache for FAQ data and embeddings
_FAQ_DATA: list[dict] = []
_FAQ_EMBEDDINGS: list[list[float]] = []

# Map FAQ source keys to (display name, URL)
SOURCE_MAP = {
    "source1": ("受講中のよくあるご質問", KNOWLEDGE_URL1),
    "source2": ("最終課題のまとめページ よくある質問（FAQ）", KNOWLEDGE_URL2),
}

# Default max tokens for completions when not using o4- models
DEFAULT_MAX_TOKENS = 150
# Default max tokens for completions when using o4- models
DEFAULT_MAX_COMPLETION_TOKENS = 150


def _load_faq():
    """
    Load FAQ entries from JSONL and compute embeddings for FAQ questions.
    """
    global _FAQ_DATA, _FAQ_EMBEDDINGS
    if not _FAQ_DATA:
        with open(FAQ_PATH, "r", encoding="utf-8") as f:
            for line in f:
                item = json.loads(line)
                # Expect each item has "question", "answer", and optionally "source"
                _FAQ_DATA.append(item)
        # Embed all FAQ questions once
        questions = [item["question"] for item in _FAQ_DATA]
        _FAQ_EMBEDDINGS = embed_texts(questions)


def embed_texts(texts: list[str]) -> list[list[float]]:
    """
    Given a list of strings, returns their embeddings as a list of vectors.
    """
    logger.info(f"[LLM: {MODEL}] embed_texts for {len(texts)} texts")
    resp = openai.embeddings.create(model=EMBED_MODEL, input=texts)
    # New API returns a CreateEmbeddingResponse with .data list
    embeddings = [d.embedding for d in resp.data]
    return embeddings


def llm_summarize_cluster(texts: list[str]) -> str:
    """
    Given a list of related texts, use the LLM to generate a single summary question.
    """
    prompt = (
        "以下の質問は類似しています。これらの質問をまとめて、コミュニティ参加者が共通に疑問に思っていることを、代表的な質問として、1つに要約してください。出力は質問だけにしてください。\n"
        + "\n".join(f"- {t}" for t in texts)
    )
    logger.info(f"[LLM: {MODEL}] llm_summarize_cluster prompt: {prompt}")  # Debug
    messages = [
        {
            "role": "system",
            "content": "あなたはコミュニティ参加者からの質問・回答をとりまとめるアシスタントです。",
        },
        {"role": "user", "content": prompt},
    ]
    create_args = {"model": MODEL, "messages": messages}
    # Always use max_tokens, choosing value by model type
    # if MODEL.startswith("o4-"):
    #    create_args["max_completion_tokens"] = DEFAULT_MAX_COMPLETION_TOKENS
    # else:
    #    create_args["max_tokens"] = DEFAULT_MAX_TOKENS

    try:
        logger.info(f"[LLM: {MODEL}] llm_summarize_cluster req: {len(texts)} texts")
        resp = openai.chat.completions.create(**create_args)
        # Log token usage if available
        usage = getattr(resp, "usage", None)
        if usage:
            logger.info(
                f"[LLM: {MODEL}] llm_summarize_cluster usage prompt_tokens={usage.prompt_tokens} completion_tokens={usage.completion_tokens} total_tokens={usage.total_tokens}"
            )
        summary = resp.choices[0].message.content.strip()
        logger.info(f"[LLM: {MODEL}] llm_summarize_cluster resp: {summary}")
        return summary
    except Exception as e:
        logger.warning(f"[LLM: {MODEL}] llm_summarize_cluster failed: {e}")
        # Fallback: return the first text
        return texts[0]


def llm_extract_topic(texts: list[str]) -> str:
    """
    Given a list of related texts, use the LLM to extract a short topic phrase or keyword.
    """
    prompt = (
        "以下の投稿は類似した投稿で、コミュニティが興味を持つトピックに関するものです。これらの投稿から、コミュニティが興味を持つトピックを表す短いフレーズを1つ抽出してください。出力は5〜8語の短いフレーズにしてください。\n"
        + "\n".join(f"- {t}" for t in texts)
    )
    logger.info(f"[LLM: {MODEL}] llm_extract_topic prompt: {prompt}")  # Debug
    messages = [
        {
            "role": "system",
            "content": "あなたはコミュニティ参加者からの質問・投稿をとりまとめ、代表的なトピックを抽出するアシスタントです。",
        },
        {"role": "user", "content": prompt},
    ]
    create_args = {"model": MODEL, "messages": messages}
    try:
        logger.info(f"[LLM: {MODEL}] llm_extract_topic req: {len(texts)} texts")
        resp = openai.chat.completions.create(**create_args)
        usage = getattr(resp, "usage", None)
        if usage:
            logger.info(
                f"[LLM: {MODEL}] llm_extract_topic usage prompt_tokens={usage.prompt_tokens} completion_tokens={usage.completion_tokens} total_tokens={usage.total_tokens}"
            )
        topic = resp.choices[0].message.content.strip()
        logger.info(f"[LLM: {MODEL}] llm_extract_topic resp: {topic}")
        return topic
    except Exception as e:
        logger.warning(f"[LLM: {MODEL}] llm_extract_topic failed: {e}")
        # Fallback: return empty string or the first text
        return texts[0] if texts else ""


# --- New functions for info request detection and extraction ---


def llm_is_info_request(texts: list[str]) -> bool:
    """
    Check whether the given cluster of texts represents an information request.
    Returns True if the ratio of info requests is >= 0.7, False otherwise.
    """
    prompt = (
        "以下の投稿は類似した投稿です。これら投稿は、コミュニティ参加者が、明確に何かを知りたい、何かを学びたいといった情報を求めるリクエストになっていかどうかを「はい」または「いいえ」で、それぞれ新しい行で回答してください。\n"
        # "以下の投稿一覧に対し、各投稿が情報リクエストかどうかを「はい」または「いいえ」で、それぞれ新しい行で回答してください。\n"
        "投稿一覧:\n"
        + "\n".join(f"- {t}" for t in texts)
    )
    logger.info(f"[LLM: {MODEL}] llm_is_info_request prompt")
    messages = [
        {
            "role": "system",
            "content": "あなたはコミュニティに投稿されるコンテンツを分類するプロフェッショナルです。",
        },
        {"role": "user", "content": prompt},
    ]
    try:
        resp = openai.chat.completions.create(model=MODEL, messages=messages)
        usage = getattr(resp, "usage", None)
        if usage:
            logger.info(
                f"[LLM: {MODEL}] llm_is_info_request usage prompt_tokens={usage.prompt_tokens} completion_tokens={usage.completion_tokens} total_tokens={usage.total_tokens}"
            )
        raw = resp.choices[0].message.content.strip()
        logger.info(f"[LLM: {MODEL}] llm_is_info_request raw: {raw}")
        lines = [line.strip() for line in raw.splitlines() if line.strip()]
        yes_count = sum(1 for line in lines if line.startswith("はい"))
        total = len(lines) if lines else len(texts)
        ratio = yes_count / total
        logger.info(
            f"[LLM: {MODEL}] llm_is_info_request yes={yes_count}, total={total}, ratio={ratio:.2f}"
        )
        return ratio >= 0.7
    except Exception as e:
        logger.warning(f"[LLM: {MODEL}] llm_is_info_request failed: {e}")
        return False


def llm_extract_info_request(texts: list[str]) -> str:
    """
    Given a list of texts confirmed as info requests, extract the core information request.
    """
    prompt = (
        "以下の投稿は情報を求めるリクエストです。何を知りたいか、何を学びたいのか、5〜8語のフレーズでまとめてください。"
        " 投稿一覧:\n" + "\n".join(f"- {t}" for t in texts)
    )
    logger.info(f"[LLM: {MODEL}] llm_extract_info_request prompt")
    messages = [
        {
            "role": "system",
            "content": "あなたはコミュニティ投稿から情報リクエストを抽出するプロフェッショナルです。",
        },
        {"role": "user", "content": prompt},
    ]
    try:
        resp = openai.chat.completions.create(model=MODEL, messages=messages)
        usage = getattr(resp, "usage", None)
        if usage:
            logger.info(
                f"[LLM: {MODEL}] llm_extract_info_request usage prompt_tokens={usage.prompt_tokens} completion_tokens={usage.completion_tokens} total_tokens={usage.total_tokens}"
            )
        request = resp.choices[0].message.content.strip()
        logger.info(f"[LLM: {MODEL}] llm_extract_info_request resp: {request}")
        return request
    except Exception as e:
        logger.warning(f"[LLM: {MODEL}] llm_extract_info_request failed: {e}")
        return ""


def rag_answer(
    query: str, post_ids: list[int], db
) -> tuple[Optional[str], Optional[str]]:
    # Local RAG using FAQ JSONL
    logger.info(f"[RAG] local FAQ RAG for query='{query}', post_ids={post_ids}")
    # Ensure FAQ data and embeddings are loaded
    _load_faq()
    # Embed the query
    query_embedding = embed_texts([query])[0]
    # Compute similarities against FAQ question embeddings
    sims = cosine_similarity([query_embedding], _FAQ_EMBEDDINGS)[0]
    # Pick top 3 FAQ entries
    top_k = 3
    top_indices = sims.argsort()[::-1][:top_k]
    # Build context from top entries with a similarity threshold
    threshold = 0.5
    selected = []
    for idx in top_indices:
        sim = sims[idx]
        if sim < threshold:
            continue
        faq = _FAQ_DATA[idx]
        source = faq.get("source", "")
        selected.append((faq["question"], faq["answer"], source, sim))
    if not selected:
        logger.info("[RAG] no FAQ entries above similarity threshold")
        return None, None
    # Format context
    context_lines = []
    sources = set()
    for q, a, src, sim in selected:
        if src:
            sources.add(src)
        context_lines.append(f"- FAQ: {q}\n  Answer: {a}")
    context = "\n".join(context_lines)
    # Build LLM prompt
    prompt = (
        f"質問: {query}\n"
        "コミュニティに関するFAQにある質問と類似する上記の質問に対して、以下の回答を参考にして、この質問に対する有用な回答を1つの文章でまとめてください。\n"
        f"{context}"
    )
    messages = [
        {
            "role": "system",
            "content": "あなたはコミュニティ参加者からの質問・回答をとりまとめるアシスタントです。",
        },
        {"role": "user", "content": prompt},
    ]
    create_args = {"model": MODEL, "messages": messages}
    # Call the LLM
    try:
        resp_llm = openai.chat.completions.create(**create_args)
        usage = getattr(resp_llm, "usage", None)
        if usage:
            logger.info(
                f"[LLM: {MODEL}] usage prompt_tokens={usage.prompt_tokens} completion_tokens={usage.completion_tokens} total_tokens={usage.total_tokens}"
            )
        answer = resp_llm.choices[0].message.content.strip()
        logger.info(f"[RAG] generated answer: {answer}")
        # Map raw source keys to display name + URL
        mapped = []
        for src in sources:
            name, url = SOURCE_MAP.get(src, (src, src))
            mapped.append(f"{name}: {url}")
        return answer, "; ".join(mapped)
    except Exception as e:
        logger.warning(f"[RAG] local RAG failed: {e}")
        return None, None


def thread_answer(post_ids: list[int], db) -> tuple[str, None]:
    """
    Aggregate Slack thread replies and refine them via LLM.
    """
    all_replies = []
    for pid in post_ids:
        replies = fetch_thread_replies(db, pid)
        all_replies.extend(replies)

    if not all_replies:
        logging.info(f"[THREAD] no thread replies for posts {post_ids}")
        return "回答が見つかりませんでした。", None

    question = fetch_post_text(db, post_ids[0]) if post_ids else ""

    combined = "\n".join(f"- {r}" for r in all_replies)
    prompt = (
        "以下のコミュニティ参加者からの質問と返信を参考に、質問に対する有用な回答を1つの文章でまとめてください。\n"
        f"質問: {question}\n" + combined
    )
    messages = [
        {
            "role": "system",
            "content": "あなたはコミュニティ参加者からの質問・回答をとりまとめるアシスタントです。",
        },
        {"role": "user", "content": prompt},
    ]
    create_args = {"model": MODEL, "messages": messages}
    # Always use max_tokens, choosing value by model type
    # if MODEL.startswith("o4-"):
    #    create_args["max_completion_tokens"] = DEFAULT_MAX_COMPLETION_TOKENS
    # else:
    #    create_args["max_tokens"] = DEFAULT_MAX_TOKENS

    try:
        logging.info(
            f"[LLM: {MODEL}] thread_answer req with {len(all_replies)} replies"
        )
        resp = openai.chat.completions.create(**create_args)
        # Log token usage if available
        usage = getattr(resp, "usage", None)
        if usage:
            logger.info(
                f"[LLM: {MODEL}] usage prompt_tokens={usage.prompt_tokens} completion_tokens={usage.completion_tokens} total_tokens={usage.total_tokens}"
            )
        answer = resp.choices[0].message.content.strip()
        logger.info(f"[LLM: {MODEL}] thread_answer resp: {answer}")
        return answer, None
    except Exception as e:
        logging.warning(f"[LLM: {MODEL}] thread_answer failed: {e}")
        return all_replies[0], None


def get_answer(query: str, post_ids: list[int], db) -> tuple[str, Optional[str]]:
    """
    Return a RAG-derived answer if found, otherwise fallback to thread-based answer.
    """
    answer, src = rag_answer(query, post_ids, db)
    if answer:
        return answer, src
    answer, _ = thread_answer(post_ids, db)
    return answer, None
