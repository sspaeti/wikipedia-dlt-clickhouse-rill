-- Create the main wikistat table for data engineering topics
CREATE TABLE IF NOT EXISTS wikistat_data_engineering
(
    time DateTime CODEC(Delta, ZSTD(3)),
    project LowCardinality(String),
    subproject LowCardinality(String),
    path String CODEC(ZSTD(3)),
    hits UInt64 CODEC(ZSTD(3)),
    ingestion_date Date DEFAULT today()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(time)
ORDER BY (path, time)
SETTINGS index_granularity = 8192;

-- Create a staging table for temporary data
CREATE TABLE IF NOT EXISTS wikistat_staging
(
    time DateTime,
    project String,
    subproject String,
    path String,
    hits UInt64
)
ENGINE = Memory;
