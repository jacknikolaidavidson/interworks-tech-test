{{ config(materialized='table') }}

select distinct
    AIRLINECODE AS "AIRLINECODE",
    TRIM(SPLIT_PART(AIRLINENAME, ':', 1)) AS "AIRLINENAME" -- 3. Removing airline code 
from {{ ref('STG_FLIGHTS') }}