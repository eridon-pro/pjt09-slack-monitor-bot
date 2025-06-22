import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

from dotenv import load_dotenv
load_dotenv()  # カレントディレクトリの .env を読み込む
import os
import glob
import sqlite3
from collections import Counter, defaultdict
import datetime
import zoneinfo
from datetime import datetime, timedelta, timezone, date
import japanize_matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import requests
import subprocess

# --- Configuration ---
# All environment variables loaded here as module-level constants
DB_PATH           = os.getenv("SCORES_DB_PATH", "scores.db")
NOTION_TOKEN      = os.getenv("NOTION_TOKEN")
NOTION_PAGE_ID    = os.getenv("NOTION_VIOLATION_PAGE_ID")
REMOTE_USER       = os.getenv("REMOTE_USER")
REMOTE_HOST       = os.getenv("REMOTE_HOST")
REMOTE_PATH       = os.getenv("REMOTE_PATH")
HOST_URL          = os.getenv("HOST_URL")
GUIDELINES_PATH   = os.getenv("GUIDELINES_PATH", "./utils/guidelines.txt")

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

PERIOD_LABELS = {
    "all":     "全期間",
    "weekly":  "過去7日間",
    "monthly": "過去30日間",
}

def upload_to_server(file_path: str):
    """
    Upload a file to the remote server via SCP.
    """
    dest = f"{REMOTE_USER}@{REMOTE_HOST}:{REMOTE_PATH}/"
    subprocess.run(["scp", file_path, dest], check=True)
    logger.info(f"Uploaded '{file_path}' to {REMOTE_HOST}:{REMOTE_PATH}")

def fetch_violation_counts(db_path: str, since_ts: float = None) -> Counter:
    """
    events テーブルから violation_rule カラムを読み込み、
    since_ts が指定されていれば ts_epoch >= since_ts の分だけ集計。
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    sql = """
        SELECT violation_rule
        FROM events
        WHERE type = 'violation'
          AND violation_rule IS NOT NULL
          AND violation_rule != ''
    """
    params = ()
    if since_ts is not None:
        sql += " AND ts_epoch >= ?"
        params = (since_ts,)
    cur.execute(sql, params)
    counter = Counter()
    for (rule_str,) in cur.fetchall():
        for r in rule_str.split(','):
            r = r.strip()
            if r:
                counter[r] += 1
    conn.close()
    return counter

def fetch_time_series_counts(db_path: str, since_ts: float = None) -> dict:
    """
    日付ごと、ルールごとの違反件数を取得。
    戻り値は {date_str: {rule: count}} の辞書。
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    sql = """
        SELECT ts_epoch, violation_rule
        FROM events
        WHERE type = 'violation'
          AND violation_rule IS NOT NULL
          AND violation_rule != ''
    """
    params = ()
    if since_ts is not None:
        sql += " AND ts_epoch >= ?"
        params = (since_ts,)
    cur.execute(sql, params)
    data = defaultdict(lambda: Counter())
    for ts_epoch, rule_str in cur.fetchall():
        dt = datetime.fromtimestamp(ts_epoch, tz=timezone.utc).astimezone(zoneinfo.ZoneInfo("Asia/Tokyo")).date()
        date_str = dt.isoformat()
        for r in rule_str.split(','):
            r = r.strip()
            if r:
                data[date_str][r] += 1
    conn.close()
    return data

def fetch_weekday_hour_heatmap(db_path: str, since_ts: float = None) -> dict:
    """
    曜日・時間帯ごとの違反件数を取得。
    戻り値は {(weekday, hour): count} の辞書。weekdayは0=月曜、hourは0-23。
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    sql = """
        SELECT ts_epoch
        FROM events
        WHERE type = 'violation'
          AND violation_rule IS NOT NULL
          AND violation_rule != ''
    """
    params = ()
    if since_ts is not None:
        sql += " AND ts_epoch >= ?"
        params = (since_ts,)
    cur.execute(sql, params)
    heatmap = defaultdict(int)
    for (ts_epoch,) in cur.fetchall():
        dt = datetime.fromtimestamp(ts_epoch, tz=timezone.utc).astimezone(zoneinfo.ZoneInfo("Asia/Tokyo"))
        weekday = dt.weekday()
        hour = dt.hour
        heatmap[(weekday, hour)] += 1
    conn.close()
    return heatmap

def plot_violation_rule_counts(counts: Counter, period_name: str, output_path: str):
    # Ensure X-axis shows all possible rule numbers from guidelines
    all_guidelines = load_guidelines(GUIDELINES_PATH)
    # Extract numeric rule IDs from guideline lines (skip header)
    all_rules = []
    for line in all_guidelines[1:]:
        # assume format 'N. description'
        num = line.split('.')[0].strip()
        if num.isdigit():
            all_rules.append(num)
    rules = sorted(all_rules, key=lambda x: int(x))
    rule_nums = [int(r) for r in rules]
    values = [counts.get(r, 0) for r in rules]

    plt.figure(figsize=(10, 6))
    plt.bar(rule_nums, values, color='red')
    # only horizontal grid lines for histogram
    plt.grid(axis='y')
    plt.xlabel("ガイドライン規約違反となったルール番号", fontsize=16)
    plt.ylabel("件数", fontsize=16)
    plt.yticks(fontsize=16)
    plt.title(f"ガイドライン規約違反となったルール番号の傾向 ({period_name})", fontsize=24)
    plt.xticks(rule_nums, rules, fontsize=16)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    logger.info(f"[{period_name}] ルール別発生件数グラフを '{output_path}' に保存しました。")

def plot_time_series(data: dict, period_name: str, output_path: str):
    # data: {date_str: {rule: count}}
    # x軸は日付、y軸は件数。ルールごとに折れ線
    all_rules = set()
    for counts in data.values():
        all_rules.update(counts.keys())
    all_rules = sorted(all_rules, key=lambda x: int(x))
    # Determine date span: daily up to yesterday
    yesterday = datetime.now(zoneinfo.ZoneInfo("Asia/Tokyo")).date() - timedelta(days=1)
    if period_name == PERIOD_LABELS["weekly"]:
        # 過去7日間を固定で表示（昨日を含む7日分）
        dates = [yesterday - timedelta(days=i) for i in reversed(range(7))]
    elif period_name == PERIOD_LABELS["monthly"]:
        # 過去30日間を固定で表示（昨日を含む30日分）
        dates = [yesterday - timedelta(days=i) for i in reversed(range(30))]
    else:
        # 全期間はデータの最小日から昨日まで連続表示
        date_objs = [datetime.strptime(d, "%Y-%m-%d").date() for d in data.keys()]
        if date_objs:
            min_date = min(date_objs)
            max_date = min(max(date_objs), yesterday)
            dates = [min_date + timedelta(days=i) for i in range((max_date - min_date).days + 1)]
        else:
            dates = []
    plt.figure(figsize=(12, 6))
    for rule in all_rules:
        y = [data[d.isoformat()].get(rule, 0) for d in dates]
        plt.plot(dates, y, marker='o', label=f"Rule {rule}")
    plt.xlabel("日時", fontsize=16)
    plt.ylabel("件数", fontsize=16)
    plt.title(f"ガイドライン規約違反となったルールの推移 ({period_name})", fontsize=24)
    plt.grid(axis='y')
    ax = plt.gca()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
    # X軸の目盛り設定：全期間は自動調整、その他は日ごと
    if period_name == PERIOD_LABELS["all"]:
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    else:
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    plt.xticks(rotation=45, fontsize=14)
    plt.yticks(fontsize=16)
    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    # Place legend inside the plot at top center, allowing multiple columns
    plt.legend(
        loc='upper center',
        bbox_to_anchor=(0.5, 0.9),
        ncol=min(len(all_rules), 5),
        fontsize=14
    )
    ax = plt.gca()
    # Set x-axis limits to span exactly from first to last date (yesterday)
    ax.set_xlim(dates[0], dates[-1])
    # Remove padding around the x-axis
    ax.margins(x=0)
    # X軸の目盛り設定：全期間は日数に応じてダイナミックに
    if period_name == PERIOD_LABELS["all"]:
        days_count = (dates[-1] - dates[0]).days + 1
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, days_count // 15)))
    plt.tight_layout(pad=0.5)
    plt.savefig(output_path)
    plt.close()
    logger.info(f"[{period_name}] 時系列グラフを '{output_path}' に保存しました。")

def plot_heatmap(heatmap: dict, period_name: str, output_path: str):
    # heatmap: {(weekday, hour): count}
    import numpy as np
    data = np.zeros((7, 24), dtype=int)
    for (weekday, hour), count in heatmap.items():
        data[weekday, hour] = count
    plt.figure(figsize=(12, 8))
    im = plt.imshow(data, aspect='auto', cmap='Reds')
    #im = plt.imshow(data, aspect='auto', cmap='terrain')

    # Set major ticks at the center of each cell
    plt.xticks(ticks=np.arange(24), labels=[f"{h}:00" for h in range(0,24)], rotation=45, fontsize=12)
    plt.yticks(ticks=np.arange(7), labels=["月", "火", "水", "木", "金", "土", "日"], fontsize=12)
    # Set minor ticks at cell boundaries
    plt.gca().set_xticks(np.arange(-.5, 24, 1), minor=True)
    plt.gca().set_yticks(np.arange(-.5, 7, 1), minor=True)
    # Draw gridlines for minor ticks (cell boundaries)
    plt.grid(which="minor", color="gray", linestyle='-', linewidth=0.5)
    # Remove default major grid
    plt.grid(False, which="major")

    # Annotate each cell with its count, centered
    for y in range(data.shape[0]):
        for x in range(data.shape[1]):
            plt.text(x, y, str(data[y, x]), ha="center", va="center", color="black" if data[y, x] < data.max()/2 else "white", fontsize=10)

    import matplotlib.ticker as ticker
    cbar = plt.colorbar(
        im,
        orientation='horizontal',
        fraction=0.1,
        pad=0.15,
        aspect=40,
        label='ガイドライン規約違反件数'
    )
    cbar.ax.tick_params(labelsize=14)
    cbar.ax.xaxis.label.set_fontsize(14)
    cbar.ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    plt.xlabel("時間帯", fontsize=16)
    plt.ylabel("曜日", fontsize=16)
    plt.title(f"ガイドライン規約違反ヒートマップ (曜日vs時間帯) ({period_name})", fontsize=24)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    logger.info(f"[{period_name}] ヒートマップを '{output_path}' に保存しました。")

def upload_image_to_notion(image_path: str) -> dict:
    """
    画像をNotionのファイルブロックとしてアップロードするためのURLを返す。
    Notion APIは直接ファイルアップロードをサポートしないため、
    ここでは画像を外部にアップロード済みでURLを取得している前提とする。
    もしアップロード済みのURLがない場合は、環境変数等でURLを指定してもよい。
    ここではローカルファイルパスをNotionの画像URLとして使えないため、
    代わりに画像をbase64エンコードしてdata URIにする方法はNotionで非推奨。
    したがって、ここでは環境変数で画像URLを指定するか、アップロード済みURLを返すようにしてください。
    """
    # 例として環境変数からURLを取得するコードをコメントで残す
    # key = os.path.basename(image_path).upper().replace('.', '_') + "_URL"
    # url = os.environ.get(key)
    # if not url:
    #     raise RuntimeError(f"Image URL for {image_path} not found in environment variable {key}")
    # return url

    # ここではNotionの外部URLにアップロード済みである前提で、画像ファイル名をURLにした簡易例を返す
    # 実際にはS3やImgur等にアップロードしてURLを取得してください
    raise RuntimeError("upload_image_to_notion() は画像アップロード済みのURLを返すように実装してください。")

def append_image_block_to_notion(page_id: str, image_url: str, caption: str = None):
    """
    指定のNotionページに画像ブロックを追加する。
    """
    url = "https://api.notion.com/v1/blocks/" + page_id + "/children"
    data = {
        "children": [
            {
                "object": "block",
                "type": "image",
                "image": {
                    "type": "external",
                    "external": {"url": image_url},
                },
            }
        ]
    }
    #if caption:
    #    data["children"][0]["image"]["caption"] = [{"type": "text", "text": {"content": caption}}]

    res = requests.patch(url, headers=HEADERS, json=data)
    if res.status_code != 200:
        raise RuntimeError(f"Failed to append image block to Notion: {res.status_code} {res.text}")
    logger.info(f"Notionページに画像を追加しました: {caption}")

def clear_page_children(page_id: str):
    """
    Delete all existing child blocks under the given Notion page.
    """
    url = f"https://api.notion.com/v1/blocks/{page_id}/children?page_size=100"
    res = requests.get(url, headers=HEADERS)
    res.raise_for_status()
    for child in res.json().get("results", []):
        del_url = f"https://api.notion.com/v1/blocks/{child['id']}"
        requests.delete(del_url, headers=HEADERS).raise_for_status()

def load_guidelines(path: str = None) -> list[str]:
    """
    Load guidelines from text file, skipping blank lines and comments.
    """
    if path is None:
        path = GUIDELINES_PATH
    rules = []
    if not os.path.exists(path):
        logger.info(f"guidelines.txt not found: {path}")
        return rules
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            rules.append(line)
    return rules

def append_guidelines_to_notion(page_id: str, guidelines: list[str]):
    """
    Overwrite the section of the given Notion page with:
    1. A Heading 2 block for the first line (header).
    2. Paragraph blocks for each subsequent rule, skipping any line that starts with "【".
    """
    # 1. Prepare blocks: first line as H2 heading, rest as paragraphs (skip lines starting with "【")
    # Use manual header instead of first line of guidelines.txt
    header = "松尾・岩澤研究室 Slackコミュニティ規約 (課題用に抜粋)"
    rules = [line for line in guidelines[1:] if not line.startswith("【")]

    children = []
    if header:
        children.append({
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [
                    {"type": "text", "text": {"content": header}}
                ]
            }
        })
    for rule in rules:
        children.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {"type": "text", "text": {"content": rule}}
                ]
            }
        })

    # 2. Append new blocks
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    res = requests.patch(url, headers=HEADERS, json={"children": children})
    if res.status_code != 200:
        raise RuntimeError(f"Failed to append guidelines to Notion: {res.status_code} {res.text}")
    logger.info("Notion page にガイドライン一覧を上書きしました。")

# --- append update timestamp to Notion ---
def append_update_timestamp(page_id: str):
    """
    Notionページ末尾に「最終更新: YYYY-MM-DD HH:MM」を追加します。
    """
    now = datetime.now(zoneinfo.ZoneInfo("Asia/Tokyo"))
    ts_str = now.strftime("%Y-%m-%d %H:%M")
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    data = {
        "children": [
            {"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":""}}]}},
            {"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":""}}]}},
            #{"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":""}}]}},
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {"type": "text", "text": {"content": f"最終更新: {ts_str}"}}
                    ]
                }
            }
        ]
    }
    res = requests.patch(url, headers=HEADERS, json=data)
    if res.status_code != 200:
        raise RuntimeError(f"Failed to append update timestamp to Notion: {res.status_code} {res.text}")
    logger.info(f"Notionページに最終更新日時を追加しました: {ts_str}")

# --- Helper: append_image_grid_to_notion ---
def append_image_grid_to_notion(page_id: str, blocks: list[tuple[str, str]]):
    """
    Append a 3x3 grid of image blocks to Notion: each element is (url, caption).
    The grid is built as a column_list block with 3 columns, each holding 3 images.
    """
    try:
        # Split blocks into 3 columns, each with 3 images (fill with empty if needed)
        columns = []
        for col_idx in range(3):
            col_imgs = []
            for row_idx in range(3):
                idx = col_idx * 3 + row_idx
                if idx < len(blocks):
                    url, caption = blocks[idx]
                    img_block = {
                        "object": "block",
                        "type": "image",
                        "image": {
                            "type": "external",
                            "external": {"url": url},
                        }
                    }
                    #if caption:
                    #    img_block["image"]["caption"] = [{"type": "text", "text": {"content": caption}}]
                    col_imgs.append(img_block)
            columns.append({
                "object": "block",
                "type": "column",
                "column": {
                    "children": col_imgs
                }
            })
        column_list_block = {
            "object": "block",
            "type": "column_list",
            "column_list": {
                "children": columns
            }
        }
        url_api = f"https://api.notion.com/v1/blocks/{page_id}/children"
        requests.patch(url_api, headers=HEADERS, json={"children": [column_list_block]}).raise_for_status()
        logger.info("Notionページに 3x3 画像グリッドを追加しました。")
    except Exception as e:
        logger.error(f"Failed to append image grid to Notion: {e}")
        raise


def main():
    if NOTION_TOKEN is None or NOTION_PAGE_ID is None:
        logger.info("環境変数 NOTION_TOKEN と NOTION_VIOLATION_PAGE_ID を設定してください。")
        return

    periods = ["weekly", "monthly", "all"]
    all_blocks = []
    for period in periods:
        # 期間のしきい値を計算
        since_ts = None
        if period == "weekly":
            since_ts = (__import__('time').time() - 7*24*3600)
        elif period == "monthly":
            since_ts = (__import__('time').time() - 30*24*3600)

        # 1. ルール別集計
        counts = fetch_violation_counts(DB_PATH, since_ts)
        if not counts:
            logger.info(f"[{period}] 違反データが見つかりませんでした。")
            # 画像生成・アップロードはスキップ、空白画像を入れない
            continue
        logger.info(f"[{period}] ルール別発生件数: {dict(counts)}")

        # 2. 時系列集計
        time_series = fetch_time_series_counts(DB_PATH, since_ts)

        # 3. 曜日・時間帯ヒートマップ
        heatmap = fetch_weekday_hour_heatmap(DB_PATH, since_ts)

        # 画像ファイル名（タイムスタンプ付き）
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = f"violation_trends_{period}_{timestamp}"
        hist_path     = f"{base_name}_hist.png"
        ts_path       = f"{base_name}_timeseries.png"
        heatmap_path  = f"{base_name}_heatmap.png"

        # プロット作成
        label = PERIOD_LABELS.get(period, period)
        plot_violation_rule_counts(counts, label, hist_path)
        plot_time_series(time_series, label, ts_path)
        plot_heatmap(heatmap, label, heatmap_path)

        # 古いファイルをリモートサーバーから削除（同じ期間のもの）
        try:
            cleanup_cmd = f"rm {REMOTE_PATH}/violation_trends_{period}_*"
            subprocess.run(["ssh", f"{REMOTE_USER}@{REMOTE_HOST}", cleanup_cmd], check=True)
            logger.info(f"Removed old remote files for period '{period}'")
        except Exception as e:
            logger.warning(f"Failed to clean up old remote files: {e}")

        # Upload images to remote server via SCP
        for path in (hist_path, ts_path, heatmap_path):
            try:
                upload_to_server(path)
            except Exception as e:
                logger.info(f"Failed to upload {path} via SCP: {e}")

        hist_url = f"{HOST_URL}/{hist_path}"
        ts_url   = f"{HOST_URL}/{ts_path}"
        heat_url = f"{HOST_URL}/{heatmap_path}"
        logger.info("Using external host URLs:")
        logger.info(f"  histogram: {hist_url}")
        logger.info(f"  timeseries: {ts_url}")
        logger.info(f"  heatmap: {heat_url}")

        # Collect for grid
        all_blocks.extend([
            (hist_url, f"ルール別発生件数 ({label})"),
            (ts_url,   f"時系列トレンド ({label})"),
            (heat_url, f"曜日・時間帯ヒートマップ ({label})"),
        ])

    # After all periods: update Notion
    clear_page_children(NOTION_PAGE_ID)
    if all_blocks:
        append_image_grid_to_notion(NOTION_PAGE_ID, all_blocks)

    try:
        guidelines = load_guidelines()
        if guidelines:
            append_guidelines_to_notion(NOTION_PAGE_ID, guidelines)
    except Exception as e:
        logger.info(f"Notion へのガイドライン一覧転記に失敗しました: {e}")

    try:
        append_update_timestamp(NOTION_PAGE_ID)
    except Exception as e:
        logger.info(f"Notionへの最終更新日時追加に失敗しました: {e}")

    # Clean up local image files
    for file in glob.glob("violation_trends_*_*.png"):
        try:
            os.remove(file)
            logger.info(f"Removed local file: {file}")
        except Exception as e:
            logger.warning(f"Failed to remove local file {file}: {e}")


if __name__ == "__main__":
    main()
