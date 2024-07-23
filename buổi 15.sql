EX2 
WITH
  card_launch AS (
  SELECT 
    card_name,
    issued_amount,
    MAKE_DATE(issue_year, issue_month, 1) AS issue_date,
    MIN(MAKE_DATE(issue_year, issue_month, 1)) OVER (
      PARTITION BY card_name) AS launch_date
  FROM monthly_cards_issued
)

SELECT 
  card_name, 
  issued_amount
FROM card_launch
WHERE issue_date = launch_date
ORDER BY issued_amount DESC;
EX1 
WITH yearly_spend_cte AS (
  SELECT 
    EXTRACT(YEAR FROM transaction_date) AS yr,
    product_id,
    SUM(spend) AS curr_year_spend,
    LAG(SUM(spend)) OVER (
      PARTITION BY product_id 
      ORDER BY EXTRACT(YEAR FROM transaction_date)
    ) AS prev_year_spend 
  FROM user_transactions
  GROUP BY product_id, EXTRACT(YEAR FROM transaction_date)
)

SELECT 
  yr,
  product_id, 
  curr_year_spend, 
  prev_year_spend, 
  ROUND(100 * 
    (curr_year_spend - prev_year_spend)
    / prev_year_spend
  , 2) AS yoy_rate 
FROM yearly_spend_cte
WHERE prev_year_spend IS NOT NULL
ORDER BY product_id, yr;
EX3 
WITH trans_num AS (
  SELECT 
    user_id, 
    spend, 
    transaction_date, 
    ROW_NUMBER() OVER (
      PARTITION BY user_id ORDER BY transaction_date) AS row_num 
  FROM transactions)
 
SELECT 
  user_id, 
  spend, 
  transaction_date 
FROM trans_num 
WHERE row_num = 3;
EX4
WITH latest_transactions_cte AS (
  SELECT 
    transaction_date, 
    user_id, 
    product_id, 
    RANK() OVER (
      PARTITION BY user_id 
      ORDER BY transaction_date DESC) AS transaction_rank 
  FROM user_transactions
)
SELECT 
  transaction_date, 
  user_id,
  COUNT(product_id) AS purchase_count
FROM latest_transactions_cte
WHERE transaction_rank = 1 
GROUP BY transaction_date, user_id
ORDER BY transaction_date;
EX5
select    
  user_id,    
  tweet_date,   
  ROUND(AVG(tweet_count) OVER (
    PARTITION BY user_id     
   order by tweet_date     
   rows between  2 PRECEDING AND CURRENT ROW)
  ,2) as rolling_avg_3d
from tweets;
EX7
WITH ranked_spending_cte AS (
  SELECT 
    category, 
    product, 
    SUM(spend) AS total_spend,
    RANK() OVER (
      PARTITION BY category 
      ORDER BY SUM(spend) DESC) AS ranking 
  FROM product_spend
  WHERE EXTRACT(YEAR FROM transaction_date) = 2022
  GROUP BY category, product
)
SELECT 
  category, 
  product, 
  total_spend 
FROM ranked_spending_cte 
WHERE ranking <= 2 
ORDER BY category, ranking;
EX8 
WITH top_10_cte AS (
  SELECT 
    artists.artist_name,
    DENSE_RANK() OVER (
      ORDER BY COUNT(songs.song_id) DESC) AS artist_rank
  FROM artists
  INNER JOIN songs
    ON artists.artist_id = songs.artist_id
  INNER JOIN global_song_rank AS ranking
    ON songs.song_id = ranking.song_id
  WHERE ranking.rank <= 10
  GROUP BY artists.artist_name
)

SELECT artist_name, artist_rank
FROM top_10_cte
WHERE artist_rank <= 5;


