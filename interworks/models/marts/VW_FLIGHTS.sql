{{ config(materialized='view') }}

select 
    -- identifier
    f.TRANSACTIONID as "TRANSACTIONID",

    -- flight metrics
    f.DISTANCEGROUP as "DISTANCEGROUP",
    f.DEPDELAYGT15 as "DEPDELAYGT15",
    f.NEXTDAYARR as "NEXTDAYARR",
    f.FLIGHTDATE as "FLIGHTDATE",
    f.DEPTIME as "DEPTIME",
    f.ARRTIME as "ARRTIME",
    f.DEPDELAY as "DEPDELAY",
    f.ARRDELAY as "ARRDELAY",
    f.DISTANCE as "DISTANCE",
    CAST(f.CANCELLED AS BOOLEAN) AS "CANCELLED",
    CAST(f.DIVERTED AS BOOLEAN) AS "DIVERTED",
    f.WHEELSOFF as "WHEELSOFF",
    f.WHEELSON as "WHEELSON",
    f.CRSARRTIME as "CRSARRTIME",

    -- airline info
    a.AIRLINENAME as "AIRLINENAME",

    -- origin info
    orig.ORIGAIRPORTNAME as "ORIGAIRPORTNAME",
    orig.ORIGINCITYNAME as "ORIGINCITYNAME",
    orig.ORIGINSTATE as "ORIGINSTATE",
    orig.ORIGINSTATENAME as "ORIGINSTATENAME",

    -- destination info
    dest.DESTAIRPORTNAME as "DESTAIRPORTNAME",
    dest.DESTCITYNAME as "DESTCITYNAME",
    dest.DESTSTATE as "DESTSTATE",
    dest.DESTSTATENAME as "DESTSTATENAME",

    -- date attributes
    d."DAY" as "DAY",
    d."WEEK" as "WEEK",
    d."MONTH" as "MONTH",
    d."QUARTER" as "QUARTER",
    d."YEAR" as "YEAR"

from {{ ref('FACT_FLIGHTS') }} f
inner join {{ ref('DIM_DATE') }} d 
    on f.FLIGHTDATE = d.FLIGHTDATE
inner join {{ ref('DIM_AIRLINE') }} a 
    on f.AIRLINECODE = a.AIRLINECODE
inner join {{ ref('DIM_AIRPORT') }} orig 
    on f.ORIGINAIRPORTCODE = orig.ORIGINAIRPORTCODE 
    and orig.AIRPORT_TYPE = 'ORIGIN'
inner join {{ ref('DIM_AIRPORT') }} dest 
    on f.DESTAIRPORTCODE = dest.DESTAIRPORTCODE 
    and dest.AIRPORT_TYPE = 'DESTINATION'


