-- Load filtered pageviews from Wikipedia dumps
-- Parameters:
--   {date_pattern}: e.g., '2025/2025-10/pageviews-20251001-*.gz'
--   {base_url}: e.g., 'https://dumps.wikimedia.org/other/pageviews/'

INSERT INTO wikistat_data_engineering (time, project, subproject, path, hits)
WITH parsed_data AS (
    SELECT
        parseDateTimeBestEffort(extract(_file, 'pageviews-([\\d\\-]+)\\.gz$')) AS time,
        splitByChar(' ', line) AS values,
        splitByChar('.', values[1]) AS projects,
        decodeURLComponent(values[2]) AS decoded_path,
        values[3] AS hits_str
    FROM url(
        '{base_url}{date_pattern}',
        LineAsString
    )
    WHERE length(values) >= 3
      AND values[2] != '-'
)
SELECT
    pd.time,
    pd.projects[1] AS project,
    pd.projects[2] AS subproject,
    pd.decoded_path AS path,
    CAST(pd.hits_str, 'UInt64') AS hits
FROM parsed_data pd
INNER JOIN data_engineering_keywords kw
    ON pd.decoded_path = kw.keyword
WHERE pd.projects[1] IN ('en', 'en.m');
