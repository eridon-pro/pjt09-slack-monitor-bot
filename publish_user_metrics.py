import logging

logging.basicConfig(
    # level=logging.DEBUG,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

from dotenv import load_dotenv

load_dotenv()  # カレントディレクトリの .env を読み込む

import os
import subprocess
import requests
import sqlite3
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator
import japanize_matplotlib
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from utils.scoring import compute_score, fetch_user_counts
from utils.slack_helpers import resolve_user
from publish_master_upsert import update_timestamp_block
from collections import defaultdict

# ─── 設定 ─────────────────────────────────────────────
DB_PATH = os.getenv("SCORES_DB_PATH", "scores.db")
TOP_N = int(os.getenv("TOP_N", "5"))
OUT_DIR = "metrics_outputs"
os.makedirs(OUT_DIR, exist_ok=True)
REMOTE_USER = os.getenv("REMOTE_USER")
REMOTE_HOST = os.getenv("REMOTE_HOST")
REMOTE_PATH = os.getenv("REMOTE_PATH")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
s3_client = boto3.client("s3", region_name=S3_REGION)
CLOUDFRONT_URL = os.getenv("CLOUDFRONT_URL")
S3_KEY_PREFIX = os.getenv("S3_KEY_PREFIX", "").strip("/")
HOST_URL = os.getenv("HOST_URL")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_PAGE_ID = os.getenv("NOTION_PAGE_ID")

# 期間定義: (ファイル名用キー, 表示ラベル, 日数)
PERIODS = [
    ("7days", "過去7日間", 7),
    ("30days", "過去30日間", 30),
    ("all", "全期間", None),
]
# プロット対象メトリクス (キー, 描画ラベル)
METRICS = [
    ("score", "貢献度スコア"),
    ("post", "投稿数"),
    ("reaction", "ポジティブリアクション数"),
    ("answer", "有用な回答数"),
    ("positive_feedback", "ポジティブFB数"),
    ("violation", "ガイドライン違反数"),
]
# キーから日本語ラベルを取得する辞書
METRICS_DICT = {k: text for k, text in METRICS}

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}


# ─── ヘルパー関数 ────────────────────────────────────────
def get_yesterday_mid():
    """日本時間ベースで昨日の 00:00 を返す"""
    today_mid = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    return today_mid - timedelta(days=1)


def get_all_start():
    """DB の最古イベント ts_epoch を datetimeで返す"""
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT MIN(ts_epoch) FROM events").fetchone()
    conn.close()
    if row and row[0]:
        # 日付を00:00に揃えて返す
        return datetime.fromtimestamp(row[0]).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    return get_yesterday_mid()


# def upload_to_server(file_path: str):
#    """SCPでリモートサーバーにファイルをアップロード"""
#    dest = f"{REMOTE_USER}@{REMOTE_HOST}:{REMOTE_PATH}/"
#    subprocess.run(["scp", file_path, dest], check=True)
#    # アップロード後、ローカルのファイルを削除
def upload_to_server(file_path: str) -> str:
    """Upload file to S3 and return public URL (optionally via CloudFront)."""
    if not S3_BUCKET:
        logger.error("S3_BUCKET not configured.")
        return ""
    base = os.path.basename(file_path)
    key = f"{S3_KEY_PREFIX}/{base}" if S3_KEY_PREFIX else base
    # --- cleanup old remote files only for this file-type+period ---
    # Filename example: metrics_7days_20250704205652.png
    filename = base
    parts = filename.split("_", 2)
    # parts[0] = "metrics" or "heatmap"
    # parts[1] = "7days" or "30days" or "all"
    type_and_period = f"{parts[0]}_{parts[1]}_"
    prefix = f"{S3_KEY_PREFIX}/{type_and_period}" if S3_KEY_PREFIX else type_and_period
    try:
        resp = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        for obj in resp.get("Contents", []):
            old_key = obj["Key"]
            if old_key.startswith(prefix) and old_key != key:
                s3_client.delete_object(Bucket=S3_BUCKET, Key=old_key)
                logger.info(f"Deleted old S3 object: {old_key}")
    except (BotoCoreError, ClientError) as e:
        logger.warning(f"Failed to cleanup old S3 objects for prefix '{prefix}': {e}")
    try:
        # s3_client.upload_file(file_path, S3_BUCKET, key, ExtraArgs={'ACL': 'public-read'})
        s3_client.upload_file(file_path, S3_BUCKET, key)
        if CLOUDFRONT_URL:
            url = f"{CLOUDFRONT_URL}/{key}"
        else:
            url = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{key}"
        logger.info(f"Uploaded {file_path} to S3://{S3_BUCKET}/{key}")
        os.remove(file_path)
        return url
    # except Exception as e:
    #    logger.warning(f"Failed to remove local file {file_path}: {e}")
    except (BotoCoreError, ClientError) as e:
        logger.error(f"S3 upload failed for {file_path}: {e}")
        return ""


def append_paragraph_to_notion(page_id: str, text: str):
    """
    Notionページに段落テキストを追加
    """
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    data = {
        "children": [
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": text}}]
                },
            }
        ]
    }
    res = requests.patch(url, headers=HEADERS, json=data)
    res.raise_for_status()


def clear_page_children(page_id: str):
    """Notionページの子ブロックをすべて削除"""
    url = f"https://api.notion.com/v1/blocks/{page_id}/children?page_size=100"
    res = requests.get(url, headers=HEADERS)
    res.raise_for_status()
    for child in res.json().get("results", []):
        block_type = child.get("type")
        # Skip embedded DB and synced blocks only
        if block_type in ("child_database", "synced_block"):
            continue
        # それ以外のブロックを削除
        del_url = f"https://api.notion.com/v1/blocks/{child['id']}"
        requests.delete(del_url, headers=HEADERS).raise_for_status()


def append_image_grid_to_notion(page_id: str, blocks: list[tuple[str, str]]):
    """
    Notionページに3列×2行グリッドで画像を追加。
    blocksは[(url, caption), ...]で6要素ある想定。
    """
    # import json
    # logger.debug(f"[DEBUG] append_image_grid_to_notion called with blocks:\n{json.dumps(blocks, ensure_ascii=False, indent=2)}")
    columns = []
    for col in range(3):
        children = []
        for row in range(2):
            idx = col * 2 + row
            url, caption = blocks[idx]
            img_block = {
                "object": "block",
                "type": "image",
                "image": {
                    "type": "external",
                    "external": {"url": url},
                },
            }
            # if caption:
            #    img_block["image"]["caption"] = [{"type":"text","text":{"content":caption}}]
            children.append(img_block)
        columns.append(
            {"object": "block", "type": "column", "column": {"children": children}}
        )
    column_list = {
        "object": "block",
        "type": "column_list",
        "column_list": {"children": columns},
    }
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    res = requests.patch(url, headers=HEADERS, json={"children": [column_list]})
    # payload = {"children": [column_list]}  # Debug
    # logger.debug(f"[DEBUG] PATCH to {url} payload:\n{json.dumps(payload, ensure_ascii=False, indent=2)}")  # Debug
    # res = requests.patch(url, headers=HEADERS, json=payload)  # Debug
    # logger.debug(f"[DEBUG] append_image_grid response: status={res.status_code}, body={res.text}")  # Debug
    res.raise_for_status()


def fetch_daily_count(
    user_id: str, metric: str, start_dt: datetime, end_dt: datetime
) -> int:
    """
    指定ユーザー・メトリクスの start_dt <= ts_epoch < end_dt の件数を返す。
    reaction は scored=1 でフィルタ。
    """
    conn = sqlite3.connect(DB_PATH)
    if metric == "reaction":
        sql = (
            "SELECT COUNT(*) FROM events "
            "WHERE user_id=? AND type='reaction' AND scored=1 "
            "AND ts_epoch>=? AND ts_epoch<?"
        )
        args = (user_id, start_dt.timestamp(), end_dt.timestamp())
    else:
        sql = (
            "SELECT COUNT(*) FROM events "
            "WHERE user_id=? AND type=? "
            "AND ts_epoch>=? AND ts_epoch<?"
        )
        args = (user_id, metric, start_dt.timestamp(), end_dt.timestamp())
    cnt = conn.execute(sql, args).fetchone()[0]
    conn.close()
    return cnt


def fetch_time_of_day_counts(user_ids, metric, start_dt, end_dt):
    """
    Returns a 7x24 array of counts for given metric and users over the time window.
    """
    # Initialize zero grid
    grid = np.zeros((7, 24), dtype=int)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # Build SQL for reaction metric
    if metric == "reaction":
        sql = """
          SELECT ts_epoch FROM events
           WHERE type='reaction' AND scored=1
             AND ts_epoch>=? AND ts_epoch<?
             AND user_id IN ({})
        """.format(
            ",".join("?" for _ in user_ids)
        )
        args = [start_dt.timestamp(), end_dt.timestamp()] + user_ids
    else:
        sql = """
          SELECT ts_epoch FROM events
           WHERE type=? 
             AND ts_epoch>=? AND ts_epoch<?
             AND user_id IN ({})
        """.format(
            ",".join("?" for _ in user_ids)
        )
        args = [metric, start_dt.timestamp(), end_dt.timestamp()] + user_ids
    cur.execute(sql, args)
    rows = cur.fetchall()
    conn.close()
    for (ts,) in rows:
        dt = datetime.fromtimestamp(ts, tz=None)
        # convert to local
        local = dt.astimezone()
        weekday = local.weekday()  # 0=Mon
        hour = local.hour
        grid[weekday, hour] += 1
    return grid


def plot_metric_trends(
    key: str, label: str, dates: list, data: dict, user_ids: list
) -> str:
    """
    折れ線グラフを描画して保存する共通関数
    """
    fig, axes = plt.subplots(len(METRICS), 1, figsize=(12, 6 * len(METRICS)))
    marker_styles = ["o", "s", "^", "D", "v", "P", "X"]
    for ax, (metric, text) in zip(axes, METRICS):
        for idx, uid in enumerate(user_ids):
            marker = marker_styles[idx % len(marker_styles)]
            ax.plot(dates, data[metric][uid], marker=marker, label=resolve_user(uid))
        ax.set_title(f"{text} の推移 ({label})", fontsize=24)
        if key == "all":
            # For long full-period plots, use up to 15 tick labels
            days = len(dates)
            interval = max(1, days // 15)
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=interval))
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
        else:
            ax.xaxis.set_major_locator(mdates.DayLocator())
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
        ax.tick_params(axis="x", rotation=45, labelsize=14)
        ax.tick_params(axis="y", labelsize=14)
        ax.grid(axis="y")
        if metric != "score":
            ax.set_ylim(bottom=0)
            ax.yaxis.set_major_locator(MaxNLocator(integer=True))
        # clamp x-axis to exact data range to remove extra padding
        ax.set_xlim(dates[0], dates[-1])
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(
            handles,
            labels,
            loc="upper center",
            ncol=len(handles),
            # bbox_to_anchor=(0.5, 0.98), fontsize='medium')
            bbox_to_anchor=(0.5, 0.98),
            fontsize=14,
        )

    plt.tight_layout()
    out_path = os.path.join(OUT_DIR, f"metrics_{key}_{datetime.now():%Y%m%d%H%M%S}.png")
    plt.savefig(out_path)
    plt.close(fig)
    print(f"[{key}] Saved Metrics Plots: {out_path}")
    return out_path


def plot_heatmap_for_period(
    key: str, label: str, start_dt: datetime, end_dt: datetime, user_ids: list
) -> str:
    """
    ヒートマップを描画して保存し、パスを返す共通関数
    """
    heat_metrics = ["post", "reaction", "answer", "positive_feedback"]
    fig_hm, axes_h = plt.subplots(
        len(heat_metrics),
        1,
        figsize=(8, 5 * len(heat_metrics)),
        constrained_layout=True,
    )
    for idx, metric in enumerate(heat_metrics):
        grid = fetch_time_of_day_counts(user_ids, metric, start_dt, end_dt)
        ax_h = axes_h[idx]
        im = ax_h.imshow(grid, aspect="auto", cmap="Reds")
        ax_h.set_title(
            f"{METRICS_DICT[metric]}ヒートマップ (曜日vs時間帯)({label})", fontsize=18
        )
        ax_h.set_yticks(np.arange(7))
        ax_h.set_yticklabels(["月", "火", "水", "木", "金", "土", "日"], fontsize=12)
        ax_h.set_xticks(np.arange(24))
        ax_h.set_xticklabels([f"{h}:00" for h in range(24)], rotation=45, fontsize=10)
        ax_h.set_xticks(np.arange(-0.5, 24, 1), minor=True)
        ax_h.set_yticks(np.arange(-0.5, 7, 1), minor=True)
        ax_h.grid(which="minor", color="gray", linestyle="-", linewidth=0.5)
        ax_h.grid(False, which="major")
        for (y, x), val in np.ndenumerate(grid):
            ax_h.text(
                x, y, str(val), ha="center", va="center", fontsize=10, color="black"
            )
    cbar = fig_hm.colorbar(
        im, ax=axes_h, orientation="horizontal", fraction=0.01, pad=0.01, location="top"
    )
    cbar.ax.xaxis.set_ticks_position("top")
    cbar.ax.xaxis.set_label_position("top")
    cbar.set_label("件数", fontsize=12)
    hm_path = os.path.join(OUT_DIR, f"heatmap_{key}_{datetime.now():%Y%m%d%H%M%S}.png")
    plt.savefig(hm_path)
    plt.close(fig_hm)
    print(f"[{key}] Saved Metrics Heatmaps: {hm_path}")
    return hm_path


# ─── メイン ──────────────────────────────────
def main():
    yesterday_mid = get_yesterday_mid()
    today_mid = yesterday_mid + timedelta(days=1)
    all_start = get_all_start()

    # 昨日24時間のスコアで TOP_N ユーザーを選出
    # fetch_user_counts のシグネチャは (db_path, since, until, limit)
    rows = fetch_user_counts(
        DB_PATH, yesterday_mid.timestamp(), today_mid.timestamp(), TOP_N
    )
    user_ids = [r[0] for r in rows]
    if not user_ids:
        print("昨日の投稿がなく、TOP_N ユーザーが取得できませんでした。")
        return

    # --- 既存のNotion埋め込みをクリア ---
    clear_page_children(NOTION_PAGE_ID)
    # --- 説明文：DB埋め込み下に表示 ---
    append_paragraph_to_notion(
        NOTION_PAGE_ID,
        "以下は、昨日の貢献度ランキングに入ったユーザーの、過去7日間、30日間、全期間の貢献度スコア、スコア構成要素の件数の推移のグラフと、それらのヒートマップです。",
    )
    # これから埋め込む画像とキャプションをまとめるリスト（3列×2行のグリッド用）
    to_embed_blocks: list[tuple[str, str]] = []

    for key, label, period_days in PERIODS:
        # 終端：昨日 00:00
        end_dt = yesterday_mid
        # 開始：全期間は all_start、固定期間は end_dt - (period_days-1) 日
        if period_days is None:
            start_dt = all_start
        else:
            cand = end_dt - timedelta(days=period_days - 1)
            start_dt = cand if cand > all_start else all_start

        # 日付リスト生成 (inclusive)
        dates = []
        d = start_dt
        while d <= end_dt:
            dates.append(d)
            d += timedelta(days=1)

        # データ構造初期化
        data = {m[0]: {uid: [] for uid in user_ids} for m in METRICS}

        # 日別カウント＆スコア計算
        for d in dates:
            ws = d
            we = d + timedelta(days=1)
            for uid in user_ids:
                for metric, _ in METRICS:
                    if metric != "score":
                        cnt = fetch_daily_count(uid, metric, ws, we)
                        data[metric][uid].append(cnt)
            # score は最後にまとめて
            for uid in user_ids:
                params = {
                    "posts": data["post"][uid][-1],
                    "reactions": data["reaction"][uid][-1],
                    "answers": data["answer"][uid][-1],
                    "positive_fb": data["positive_feedback"][uid][-1],
                    "violations": data["violation"][uid][-1],
                }
                data["score"][uid].append(compute_score(params))

        # 折れ線グラフを作成し、パスを得る
        out_path = plot_metric_trends(key, label, dates, data, user_ids)
        # ヒートマップを作成し、パスを得る
        hm_path = plot_heatmap_for_period(key, label, start_dt, end_dt, user_ids)

        # --- 古いリモートファイルを削除 (新しいアップロード前) ---
        # try:
        #    cleanup_cmd = f"rm {REMOTE_PATH}/metrics_{key}_* {REMOTE_PATH}/heatmap_{key}_*"
        #    subprocess.run(
        #        ["ssh", f"{REMOTE_USER}@{REMOTE_HOST}", cleanup_cmd],
        #        check=True
        #    )
        #    logger.info(f"Removed old remote metrics and heatmap files for period '{key}'")
        # except Exception as e:
        #    logger.warning(f"Failed to cleanup old remote files: {e}")
        # SCP-based cleanup and upload removed

        # SCPで両画像をアップロード
        # upload_to_server(out_path)
        # upload_to_server(hm_path)
        # public_line_url = f"{HOST_URL}/{os.path.basename(out_path)}"
        # public_hm_url   = f"{HOST_URL}/{os.path.basename(hm_path)}"
        # S3 upload and capture URLs
        public_line_url = upload_to_server(out_path)
        public_hm_url = upload_to_server(hm_path)
        # 埋め込み用URLとキャプションをリストに追加（グリッド用）
        to_embed_blocks.append((public_line_url, f"{label} 折れ線グラフ"))
        to_embed_blocks.append((public_hm_url, f"{label} ヒートマップ"))

    # --- Notionにまとめてグリッド埋め込み ---
    if to_embed_blocks:
        append_image_grid_to_notion(NOTION_PAGE_ID, to_embed_blocks)

    # --- 最終更新日時を更新 ---
    append_paragraph_to_notion(NOTION_PAGE_ID, "\n")
    update_timestamp_block()


if __name__ == "__main__":
    main()
