SELECT 
    hour,
    article,
    SUM(view_count) as total_views
FROM read_parquet('s3://wikimedia-pageviews-parquet/year=2024/month=*/day=*/hour=*/*.parquet')
WHERE article LIKE '%data%engineering%'
   OR article LIKE '%Apache_Spark%'
   OR article LIKE '%ETL%'
   OR article LIKE '%Airflow%'
GROUP BY hour, article
ORDER BY total_views DESC
LIMIT 100
