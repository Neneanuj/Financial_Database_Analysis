SELECT
    cik,
    filing_date,
    SUM(CASE 
        WHEN metric_name = 'Assets' THEN value_clean 
        ELSE 0 
    END) AS total_assets,
    SUM(CASE 
        WHEN metric_name = 'Liabilities' THEN value_clean 
        ELSE 0 
    END) AS total_liabilities,
    SUM(CASE 
        WHEN metric_name = 'Equity' THEN value_clean 
        ELSE 0 
    END) AS equity
FROM {{ ref('cleaned_normalized') }}
GROUP BY 1,2