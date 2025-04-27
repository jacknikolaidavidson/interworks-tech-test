/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing table to view below
*/

{{ config(materialized='table') }}

select 
    TRANSACTIONID ,
    FLIGHTDATE ,
    AIRLINECODE ,
    AIRLINENAME ,
    TAILNUM ,
    FLIGHTNUM ,
    ORIGINAIRPORTCODE ,
    ORIGAIRPORTNAME ,
    ORIGINCITYNAME ,
    ORIGINSTATE ,
    ORIGINSTATENAME ,
    DESTAIRPORTCODE ,
    DESTAIRPORTNAME ,
    DESTCITYNAME ,
    DESTSTATE ,
    DESTSTATENAME ,
    CRSDEPTIME ,
    DEPTIME ,
    DEPDELAY ,
    TAXIOUT ,
    WHEELSOFF ,
    WHEELSON ,
    TAXIIN ,
    CRSARRTIME ,
    ARRTIME ,
    ARRDELAY ,
    CRSELAPSEDTIME ,
    ACTUALELAPSEDTIME ,
    CANCELLED ,
    DIVERTED ,
    DISTANCE 
from {{ source('RECRUITMENT_DB.CANDIDATE_00200', 'RAW_FLIGHTS') }}