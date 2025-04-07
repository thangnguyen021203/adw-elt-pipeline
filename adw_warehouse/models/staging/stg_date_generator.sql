WITH calendar AS (
    SELECT
      SEQ4() AS i
    FROM TABLE(GENERATOR(ROWCOUNT => 14000))
)

SELECT
  DATEADD(DAY, i, '1990-01-01') AS full_date
FROM calendar
