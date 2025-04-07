WITH calendar AS (
  SELECT SEQ4() AS i
  FROM TABLE(GENERATOR(ROWCOUNT => 40 * 365 * 24 * 60 * 60))  -- 20 năm * 365 ngày * 24h * 60p * 60s
)

SELECT
  DATEADD(SECOND, i, '1990-01-01 00:00:00')::TIMESTAMP_NTZ(0) AS full_datetime
FROM calendar
