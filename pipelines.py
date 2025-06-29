import os
import json
from datetime import datetime, timedelta, timezone
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

from dotenv import load_dotenv
load_dotenv()
FAQ_WINDOW_DAYS = int(os.environ.get("FAQ_WINDOW_DAYS", 7))
QUESTION_CH     = os.environ["QUESTION_CHANNEL"]   # bot-qa-dev
DEV_CH          = os.environ["BOT_DEV_CHANNEL"]    # bot-dev

from sklearn.cluster import DBSCAN, AgglomerativeClustering
from sklearn.metrics import silhouette_score

from utils.db import (
    fetch_posts_for_faq,
    fetch_last_import_ts,
    count_posts_since,
    fetch_posts_for_topics,
    # fetch_posts_for_info,
    insert_extracted_item,
    insert_extracted_item_type,
    insert_trend_topic,
    insert_info_request,
)
from utils.llm_helpers import (
    embed_texts,
    llm_summarize_cluster,
    llm_extract_topic,
    llm_is_info_request,
    llm_extract_info_request,
    thread_answer,
    get_answer,
)


def process_faq(db):
    """
    1) bot-qa-dev チャンネルの親スレッド（質問）のみをクラスタリングして代表質問を生成
    2) RAG or スレッド回答の集約による回答生成
    3) extracted_items, extracted_item_types テーブルに登録
    """
    logger.info("process_faq() called")
    # Threshold check: skip processing if too few new posts
    last_ts = fetch_last_import_ts(db)
    new_count = count_posts_since(db, last_ts)
    THRESHOLD = 10
    if new_count < THRESHOLD:
        logger.info(f"[FAQ] Skipping FAQ processing: only {new_count} new posts (< threshold {THRESHOLD}).")
        return
    logger.info(f"[FAQ] Starting FAQ processing: {new_count} new posts (>= threshold {THRESHOLD}).")
    posts = fetch_posts_for_faq(db, QUESTION_CH, FAQ_WINDOW_DAYS)
    # 質問のみ（親スレッド）を対象にする
    posts = [p for p in posts if p["thread_ts"] == p["ts"]]
    if not posts:
        logger.info("No FAQ question posts found, skipping.")
        return

    # Use latest post timestamp as the reference for the cutoff
    max_ts = max(p["ts"] for p in posts)
    cutoff_ts = max_ts - FAQ_WINDOW_DAYS * 86400
    filtered = []
    for p in posts:
        if p["ts"] >= cutoff_ts:
            filtered.append(p)
    logger.info(f"[FAQ] total fetched question posts: {len(posts)}, after window filter: {len(filtered)} (using max_ts={max_ts})")
    if not filtered:
        logger.info("No FAQ question posts in window, skipping.")
        return
    posts = filtered

    question_texts = [p["text"] for p in posts]
    question_ids  = [p["id"]   for p in posts]

    embeddings = embed_texts(question_texts)

    # Perform hierarchical clustering (AgglomerativeClustering) with a distance threshold
    agg = AgglomerativeClustering(
        n_clusters=None,
        distance_threshold=0.6,  # adjust this threshold for cluster granularity
        metric='cosine',
        linkage='average'
    )
    best_labels = agg.fit_predict(embeddings)
    logger.info(f"[FAQ] Agglomerative clustering produced {len(set(best_labels)) - (1 if -1 in best_labels else 0)} clusters (excluding noise)")

    if best_labels is None:
        logger.info("Could not find suitable clustering, treating all as one cluster")
        best_labels = [0] * len(question_texts)

    n_clusters = len(set(best_labels)) - (1 if -1 in best_labels else 0)
    logger.info(f"[FAQ] final cluster count: {n_clusters}")

    clusters = {}
    for idx, label in enumerate(best_labels):
        if label == -1:
            # Treat noise points as separate clusters
            label = max(clusters.keys(), default=-1) + 1
        clusters.setdefault(label, {"post_ids": [], "texts": []})
        clusters[label]["post_ids"].append(question_ids[idx])
        clusters[label]["texts"].append(question_texts[idx])

    # Remove clusters with only a single post (treat as noise)
    singleton_ids = [cid for cid, c in clusters.items() if len(c["post_ids"]) < 2]
    for cid in singleton_ids:
        logger.info(f"[FAQ] skipping singleton cluster {cid} with only {len(clusters[cid]['post_ids'])} post")
        clusters.pop(cid)

    # Log count of usable clusters
    usable_count = len(clusters)
    logger.info(f"[FAQ] usable clusters after removing singletons: {usable_count}")

    for cluster_id, cluster in clusters.items():
        logger.info(f"[FAQ] Summarizing cluster {cluster_id} with {len(cluster['texts'])} posts")
        summary = llm_summarize_cluster(cluster["texts"])
        logger.info(f"[FAQ] cluster summary: {summary}")

        # Use the cluster summary as the RAG query
        answer, source_url = get_answer(summary, cluster["post_ids"], db)
        logger.info(f"[FAQ] generated answer: {answer} (source: {source_url})")

        try:
            logger.info(f"[FAQ] inserting extracted item for cluster {cluster_id}")
            item_id = insert_extracted_item(
                cluster["post_ids"],
                summary,
                datetime.utcnow().timestamp(),
                answer,
                source_url,
            )
            logger.info(f"[FAQ] inserted extracted_item id={item_id} for cluster {cluster_id}")
            insert_extracted_item_type(item_id, "faq")
            logger.info(f"[FAQ] inserted extracted_item_type for item_id={item_id}, type=faq")
        except Exception as e:
            logger.error(f"[FAQ] failed to insert extracted item for cluster {cluster_id}: {e}")


def process_trend_topics(db):
    """
    bot-dev + bot-qa-dev の投稿から、コミュニティが興味を持つトピック上位5件を抽出する。
    クラスタリングし、3件以上の投稿を含むクラスタをサイズ順に上位5つ選択。
    """
    logger.info("process_trend_topics() called")
    TOPIC_CHANNELS = [DEV_CH, QUESTION_CH]  # 例: ["bot-dev", "bot-qa-dev"]
    last_ts = fetch_last_import_ts(db)
    new_count = count_posts_since(db, last_ts)
    THRESHOLD = 10
    if new_count < THRESHOLD:
        logger.info(f"[Topics] Skipping trend topics: only {new_count} new posts (< threshold {THRESHOLD}).")
        return
    logger.info(f"[Topics] Starting trend topics processing: {new_count} new posts (>= threshold {THRESHOLD}).")
    posts = fetch_posts_for_topics(db, TOPIC_CHANNELS, FAQ_WINDOW_DAYS)
    if not posts:
        logger.info("No topic posts found, skipping.")
        return

    texts = [p["text"] for p in posts]
    post_ids = [p["id"] for p in posts]

    # Embed all post texts
    embeddings = embed_texts(texts)

    # Hierarchical clustering with cosine threshold
    agg = AgglomerativeClustering(
        n_clusters=None,
        distance_threshold=0.6,
        metric='cosine',
        linkage='average'
    )
    labels = agg.fit_predict(embeddings)

    # Group texts by cluster label
    clusters = {}
    for idx, label in enumerate(labels):
        clusters.setdefault(label, []).append(texts[idx])

    # Log each cluster's ID and its post texts
    for cid, texts_in_cluster in clusters.items():
        logger.info(f"[Topics] cluster {cid} posts: {texts_in_cluster}")

    # Filter clusters with at least 3 posts
    valid_clusters = [(label, texts) for label, texts in clusters.items() if len(texts) >= 3]
    if not valid_clusters:
        logger.info("No clusters with minimum size 3, skipping.")
        return

    # Sort clusters by size descending
    valid_clusters.sort(key=lambda x: len(x[1]), reverse=True)
    # Log each cluster label with its size, casting labels to plain ints
    cluster_sizes = ", ".join(f"{int(label)}:{len(texts)}" for label, texts in valid_clusters)
    logger.info(f"[Topics] cluster label:size: {cluster_sizes}")
    # Log cluster labels in descending size order as standard ints
    ordered_labels = [int(label) for label, _ in valid_clusters]
    logger.info(f"[Topics] clusters ordered by size: {ordered_labels}")
    # Take top 5
    top_clusters = valid_clusters[:5]

    # Summarize each top cluster as a topic
    for label, cluster_texts in top_clusters:
        topic_summary = llm_extract_topic(cluster_texts)
        logger.info(f'[Topics] extracted topic: {topic_summary}, cluster={label}, size={len(cluster_texts)}')
        try:
            logger.info(f"[Topics] inserting trend_topic for cluster={int(label)}, size={len(cluster_texts)}, topic={topic_summary}")
            topic_id = insert_trend_topic(db, int(label), topic_summary, len(cluster_texts))
            logger.info(f"[Topics] inserted trend_topic id={topic_id}, cluster={label}, size={len(cluster_texts)}, topic={topic_summary}")
        except Exception as e:
            logger.error(f"[Topics] failed to insert trend_topic for cluster={label}, size={len(cluster_texts)}, topic={topic_summary}: {e}")



def process_info_requests(db):
    """
    bot-dev + bot-qa-dev の投稿から、多くの人が求めている"情報リクエスト"を抽出し登録
    クラスタリングで3件以上の親スレッドを含むクラスターごとにLLMでリクエストフレーズを抽出してログに出力
    """
    logger.info("process_info_requests() called")
    TOPIC_CHANNELS = [DEV_CH, QUESTION_CH]
    last_ts = fetch_last_import_ts(db)
    new_count = count_posts_since(db, last_ts)
    THRESHOLD = 10
    #if new_count < THRESHOLD:
    #    logger.info(f"[InfoRequests] Skipping info requests: only {new_count} new posts (< threshold {THRESHOLD}).")
    #    return
    logger.info(f"[InfoRequests] Starting info requests processing: {new_count} new posts (>= threshold {THRESHOLD}).")
    posts = fetch_posts_for_topics(db, TOPIC_CHANNELS, FAQ_WINDOW_DAYS)
    if not posts:
        logger.info("No topic posts found, skipping.")
        return

    texts = [p["text"] for p in posts]
    post_ids = [p["id"] for p in posts]

    # Embed all post texts
    embeddings = embed_texts(texts)

    # Hierarchical clustering with cosine threshold
    agg = AgglomerativeClustering(
        n_clusters=None,
        distance_threshold=0.6,
        metric='cosine',
        linkage='average'
    )
    labels = agg.fit_predict(embeddings)
    # Group posts by cluster label
    clusters = {}
    for idx, label in enumerate(labels):
        clusters.setdefault(label, {"post_ids": [], "texts": []})
        clusters[label]["post_ids"].append(post_ids[idx])
        clusters[label]["texts"].append(texts[idx])
    # Filter clusters with at least 3 posts
    valid_clusters = [(label, c) for label, c in clusters.items() if len(c["texts"]) >= 3]
    if not valid_clusters:
        logger.info("No info-request clusters with minimum size 3, skipping.")
        return
    # Sort clusters by size descending
    valid_clusters.sort(key=lambda x: len(x[1]["texts"]), reverse=True)
    cluster_sizes = ", ".join(f"{int(label)}:{len(c['texts'])}" for label, c in valid_clusters)
    logger.info(f"[InfoRequests] cluster label:size: {cluster_sizes}")
    for label, cluster in valid_clusters:
        logger.info(f"[InfoRequests] summarizing cluster {label} with {len(cluster['texts'])} posts")
        # Step 1: determine if this is an information request
        is_request = llm_is_info_request(cluster["texts"])
        logger.info(f"[InfoRequests] cluster {label} is_info_request? {is_request}")
        if not is_request:
            logger.info(f"[InfoRequests] skipping cluster {label} as not an information request")
            continue
        # Step 2: extract the specific request phrase as JSON list
        raw = llm_extract_info_request(cluster["texts"])
        logger.info(f"[InfoRequests] LLM raw output:\n{raw}")
        # Parse each non-empty line, strip leading bullets or dashes
        items = [
            line.lstrip("‐-・ ").strip()
            for line in raw.splitlines()
            if line.strip()
        ]
        payload = json.dumps(items, ensure_ascii=False)
        logger.info(f"[InfoRequests] items as JSON: {payload}")
        try:
            logger.info(f"[InfoRequests] inserting info_request for cluster={label}, size={len(cluster['texts'])}")
            req_id = insert_info_request(
                db,
                int(label),
                payload,
                len(cluster["texts"])
            )
            logger.info(f"[InfoRequests] inserted info_request id={req_id}, cluster={label}, size={len(cluster['texts'])}")
        except Exception as e:
            logger.error(f"[InfoRequests] failed to insert info_request for cluster {label}: {e}")


