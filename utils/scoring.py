import sqlite3
from typing import Dict, List, Tuple
from .constants import WEIGHTS


def compute_score(counts: Dict[str, float]) -> float:
    """
    与えられた各種カウントからスコアを計算する。

    counts = {
      'posts': int,
      'reactions': int,      # scored=1 のものだけ
      'answers': int,
      'positive_fb': int,
      'violations': int
    }
    """
    return (
        counts.get("posts", 0) * WEIGHTS["post"]
        + counts.get("reactions", 0) * WEIGHTS["reaction"]
        + counts.get("answers", 0) * WEIGHTS["answer"]
        + counts.get("positive_fb", 0) * WEIGHTS["positive_feedback"]
        + counts.get("violations", 0) * WEIGHTS["violation"]
    )


def fetch_user_counts(
    db_path: str, since: float, until: float, limit: int = 5
) -> List[Tuple[str, int, int, int, int, int, float]]:
    """
    SQLite の events テーブルから、指定期間のユーザーごとの各種カウントとスコアを取得する。

    Returns a list of tuples:
      (user_id, posts, reactions, answers, positive_fb, violations, score)
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          user_id,
          SUM(CASE WHEN type='post'              THEN 1 ELSE 0 END)           AS posts,
          SUM(CASE WHEN type='reaction' AND scored=1 THEN 1 ELSE 0 END)      AS reactions,
          SUM(CASE WHEN type='answer'            THEN 1 ELSE 0 END)          AS answers,
          SUM(CASE WHEN type='positive_feedback' THEN 1 ELSE 0 END)          AS positive_fb,
          SUM(CASE WHEN type='violation'         THEN 1 ELSE 0 END)          AS violations,
          (
            SUM(CASE WHEN type='post'              THEN 1 ELSE 0 END) * ?
          + SUM(CASE WHEN type='reaction' AND scored=1 THEN 1 ELSE 0 END) * ?
          + SUM(CASE WHEN type='answer'            THEN 1 ELSE 0 END) * ?
          + SUM(CASE WHEN type='positive_feedback' THEN 1 ELSE 0 END) * ?
          + SUM(CASE WHEN type='violation'         THEN 1 ELSE 0 END) * ?
          ) AS score
        FROM events
        WHERE ts_epoch >= ? AND ts_epoch < ?
        GROUP BY user_id
        ORDER BY score DESC
        LIMIT ?
    """,
        (
            WEIGHTS["post"],
            WEIGHTS["reaction"],
            WEIGHTS["answer"],
            WEIGHTS["positive_feedback"],
            WEIGHTS["violation"],
            since,
            until,
            limit,
        ),
    )
    rows = cur.fetchall()
    conn.close()
    # each row is (user_id, posts, reactions, answers, positive_fb, violations, score)
    return rows
