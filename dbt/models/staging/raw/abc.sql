models:
  staging:
    raw:
      - name: stg_raw_sub
        columns:
          - name: adsh
            tests:
              - unique
              - not_null
          - name: cik
            tests:
              - not_null
    
    normalized:
      - name: stg_normalized_sub
        tests:
          - dbt_utils.unique_combination_of_columns:
              combination_of_columns:
                - adsh
                - tag
