WITH sub AS (
    SELECT 
        adsh, 
        cik, 
        period AS filing_date
    FROM {{ source('raw', 'sub') }}  -- sub.txt
),

num AS (
    SELECT 
        adsh, 
        tag, 
        value, 
        ddate AS metric_date
    FROM {{ source('raw', 'num') }}  -- num.txt
),

pre AS (
    SELECT 
        adsh, 
        tag, 
        uom 
    FROM {{ source('raw', 'pre') }}  -- pre.txt
),

tag_def AS (
    SELECT 
        tag, 
        label 
    FROM {{ source('raw', 'tag') }}  -- tag.txt
)

SELECT
    sub.cik,
    sub.filing_date,
    num.metric_date,
    tag_def.label AS metric_name,
    num.value,
    pre.uom
FROM num
JOIN sub USING (adsh)
JOIN pre USING (adsh, tag)
JOIN tag_def USING (tag)