-- This staging model cleans and standardizes the raw balance_sheet data from SEC.
with raw_data as (
    select
        ticker,
        cik,
        filing_date,
        fiscal_year,
        fiscal_period,
        tag,
        value,
        unit
    from {{ source('sec', 'balance_sheet') }}
)

select
    ticker,
    cik,
    filing_date,
    fiscal_year,
    fiscal_period,
    tag,
    value,
    unit
from raw_data;
{{ 
  config(
    materialized='view',
    alias='stg_balance_sheet'
  )
}}

with source as (
  select * from {{ source('staginghemarts', 'balance_sheet') }}
),

renamed as (
  select
    -- 主键和维度
    ticker::varchar(10) as ticker,
    cik::varchar(20) as cik,
    filing_date::date as filing_date,
    fiscal_year::integer as fiscal_year,
    fiscal_period::varchar(2) as fiscal_period,
    
    -- 度量值和标签
    tag::varchar(100) as financial_tag,
    value::float as value,
    uom::varchar(10) as unit_of_measure
   
    
  from source
)

select * from renamed