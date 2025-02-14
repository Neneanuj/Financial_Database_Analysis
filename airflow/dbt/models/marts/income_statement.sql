SELECT
    cik,
    EXTRACT(YEAR FROM filing_date) AS fiscal_year,
    SUM(CASE 
        WHEN metric_name IN ('Revenue', 'SalesRevenueNet') THEN value_clean 
        ELSE 0 
    END) AS revenue,
    SUM(CASE 
        WHEN metric_name LIKE '%Expense%' THEN value_clean 
        ELSE 0 
    END) AS operating_expenses,
    revenue - operating_expenses AS net_income
FROM {{ ref('cleaned_normalized') }}
GROUP BY 1,2