-- Create the keywords table for filtering
CREATE TABLE IF NOT EXISTS data_engineering_keywords
(
    keyword String,
    category LowCardinality(String),
    added_date Date DEFAULT today()
)
ENGINE = MergeTree
ORDER BY keyword;
