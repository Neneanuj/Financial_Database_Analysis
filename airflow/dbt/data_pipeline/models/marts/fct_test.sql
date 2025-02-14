select
    subnum.adsh,
    subnum.cik,
    subnum.tag
from {{ ref('stg_sec_subnum') }} as subnum
