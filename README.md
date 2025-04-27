
## Setup
virtualenv venv source ./venv/bin/activate pip install -r requirements.txt
cd interworks
dbt run


## Approach:
### 1. Data loading

```
-- create pipe format for querying source
CREATE OR REPLACE FILE FORMAT CANDIDATE_00200.PIPE_FORMAT_NO_SKIP
  TYPE = 'CSV'
  FIELD_DELIMITER = '|'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  COMPRESSION = 'gzip';

--query source gzip to get attributes
SELECT     $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
    $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31
FROM @RECRUITMENT_DB.PUBLIC.S3_FOLDER/flights.gz
(FILE_FORMAT => 'CANDIDATE_00200.PIPE_FORMAT_NO_SKIP')
LIMIT 10;

-- create table 
CREATE OR REPLACE TABLE RECRUITMENT_DB.CANDIDATE_00200.RAW_FLIGHTS (
    "TRANSACTIONID" NUMBER,
    "FLIGHTDATE" DATE,
    "AIRLINECODE" STRING,
    "AIRLINENAME" STRING,
    "TAILNUM" STRING,
    "FLIGHTNUM" STRING,
    "ORIGINAIRPORTCODE" STRING,
    "ORIGAIRPORTNAME" STRING,
    "ORIGINCITYNAME" STRING,
    "ORIGINSTATE" STRING,
    "ORIGINSTATENAME" STRING,
    "DESTAIRPORTCODE" STRING,
    "DESTAIRPORTNAME" STRING,
    "DESTCITYNAME" STRING,
    "DESTSTATE" STRING,
    "DESTSTATENAME" STRING,
    "CRSDEPTIME" STRING,
    "DEPTIME" STRING,
    "DEPDELAY" NUMBER,
    "TAXIOUT" NUMBER,
    "WHEELSOFF" STRING,
    "WHEELSON" STRING,
    "TAXIIN" NUMBER,
    "CRSARRTIME" STRING,
    "ARRTIME" STRING,
    "ARRDELAY" NUMBER,
    "CRSELAPSEDTIME" NUMBER,
    "ACTUALELAPSEDTIME" NUMBER,
    "CANCELLED" STRING,
    "DIVERTED" STRING,
    "DISTANCE" STRING
);

-- copy into table
COPY INTO RECRUITMENT_DB.CANDIDATE_00200.RAW_FLIGHTS
FROM @RECRUITMENT_DB.PUBLIC.S3_FOLDER/flights.gz
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' COMPRESSION = 'gzip' SKIP_HEADER = 1);
```

### 2. Data modelling
FACT_FLIGHTS
    Flight events, metrics, keys
    Analytical center
DIM_AIRLINE
    Airline names
    Lookup for airlines
DIM_AIRPORT
    Airport and city/state info
    Lookup for airports
DIM_DATE 
    Calendar breakdown
    Easier date analysis

### 3. Data transformation
- Add DISTANCEGROUP with proper 100-mile bins
- Create DEPDELAYGT15 indicator (0/1)
- Create NEXTDAYARR indicator (0/1)
- Proper data type conversions
- Proper date hierarchy for analysis (year, quarter, month, day)
- Clean AIRLINENAME by removing airline code
- Clean ORIGAIRPORTNAME/DESTAIRPORTNAME by removing city/state
- Normalize and deduplicate dimension data

### 4. Data quality 
- Address the time format issue with 2400 time values (convert to 0000)
- Handle 0 values in CRS fields by setting as null, this should've been done by calculating from ARRTIME+ARRDELAY but I ran out of time to handle the 24-hour wraparound so I've set to null.
- There are issues with DEPDELAY/ARRDELAY if CRSDEPTIME and DEPTIME are accurate, I couldn't solve this 
in time as it involves accounting for early/late departures as well as actual departure times taking place the day after scheduled departure times
- TAILNUM is missing for 13.16% records
- TAXIOUT is missing for 13.02% records where the flight wasn't cancelled
- WHEELSOFF is missing for 13.02% records where the flight wasn't cancelled
- WHEELSON is missing for 13.15% records where the flight wasn't cancelled
- TAXIIN is missing for 13.14% records where the flight wasn't cancelled

### 5. View Creation
Create VW_FLIGHTS joining fact and dimension tables
Include all required columns in requested format
Ensure proper quoting for case-sensitivity