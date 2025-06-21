
.schema

.schema events

.schema reaction_judgement

SELECT * FROM events;

SELECT * FROM reaction_judgement;


-- ユーザーごとの各種件数出力（昨日）
SELECT
  user_id,
  -- 投稿数
  SUM(CASE WHEN type = 'post' THEN 1 ELSE 0 END) AS post_count,
  -- リアクション数（scored=1 のもののみ）
  SUM(CASE WHEN type = 'reaction' AND scored = 1 THEN 1 ELSE 0 END) AS reaction_count,
  -- 回答数
  SUM(CASE WHEN type = 'answer' THEN 1 ELSE 0 END) AS answer_count,
  -- ポジティブフィードバック数
  SUM(CASE WHEN type = 'positive_feedback' THEN 1 ELSE 0 END) AS positive_feedback_count,
  -- 違反数
  SUM(CASE WHEN type = 'violation' THEN 1 ELSE 0 END) AS violation_count
FROM events
WHERE
  -- SQLite の date 関数で「ts_epoch を unixepoch → localtime 変換後の日付」が
  -- 「昨日」の日付とピッタリ一致するものだけを抽出
  date(datetime(ts_epoch, 'unixepoch'), 'localtime') = date('now', 'localtime', '-1 day')
GROUP BY
  user_id
ORDER BY
  post_count DESC;



 -- ユーザーごとの各種件数出力（今日）
SELECT
  user_id,
  SUM(CASE WHEN type = 'post' THEN 1 ELSE 0 END) AS post_count,
  SUM(CASE WHEN type = 'reaction' AND scored = 1 THEN 1 ELSE 0 END) AS reaction_count,
  SUM(CASE WHEN type = 'answer' THEN 1 ELSE 0 END) AS answer_count,
  SUM(CASE WHEN type = 'positive_feedback' THEN 1 ELSE 0 END) AS positive_feedback_count,
  SUM(CASE WHEN type = 'violation' THEN 1 ELSE 0 END) AS violation_count
FROM events
WHERE
  date(datetime(ts_epoch, 'unixepoch'), 'localtime') = date('now', 'localtime')
GROUP BY
  user_id
ORDER BY
  post_count DESC;
 

 -- ユーザーごとの各種件数出力（昨日から30日間） -30 を　 -7 に変えれば8日間
SELECT
  user_id,
  SUM(CASE WHEN type = 'post'              THEN 1 ELSE 0 END) AS post_count,
  SUM(CASE WHEN type = 'reaction' AND scored = 1 THEN 1 ELSE 0 END) AS reaction_count,
  SUM(CASE WHEN type = 'answer'            THEN 1 ELSE 0 END) AS answer_count,
  SUM(CASE WHEN type = 'positive_feedback' THEN 1 ELSE 0 END) AS positive_feedback_count,
  SUM(CASE WHEN type = 'violation'         THEN 1 ELSE 0 END) AS violation_count
FROM events
WHERE
  date(datetime(ts_epoch, 'unixepoch'), 'localtime')
  BETWEEN date('now','localtime','-30 days')
      AND date('now','localtime','-1 day')
GROUP BY
  user_id
ORDER BY
  post_count DESC;


 -- ユーザーごとの各種件数出力（昨日から全期間）
SELECT
  user_id,
  SUM(CASE WHEN type = 'post'              THEN 1 ELSE 0 END) AS post_count,
  SUM(CASE WHEN type = 'reaction' AND scored = 1 THEN 1 ELSE 0 END) AS reaction_count,
  SUM(CASE WHEN type = 'answer'            THEN 1 ELSE 0 END) AS answer_count,
  SUM(CASE WHEN type = 'positive_feedback' THEN 1 ELSE 0 END) AS positive_feedback_count,
  SUM(CASE WHEN type = 'violation'         THEN 1 ELSE 0 END) AS violation_count
FROM events
WHERE
  -- 昨日までの全データを日付文字列比較で抽出
  date(datetime(ts_epoch, 'unixepoch'), 'localtime') <= date('now','localtime','-1 day')
GROUP BY
  user_id
ORDER BY
  post_count DESC;


--- 最古のデータが登録された日付
SELECT
  date( datetime( MIN(ts_epoch), 'unixepoch'), 'localtime' ) AS earliest_date
FROM events;
