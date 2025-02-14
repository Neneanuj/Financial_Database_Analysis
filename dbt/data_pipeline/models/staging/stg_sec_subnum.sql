select
    adsh,
    cik,
    tag,
    value
from {{ source('json_data', 'json_sec_data') }}