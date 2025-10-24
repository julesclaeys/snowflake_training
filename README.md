
# Transform Project: Amplitude in Snowflake
### Week 3 | Monday | Instructor Led Walkthrough

## Introduction

## Learning Objectives
- Dealing with nested arrays
- Applying Medallion Architecture Pattern 
- Stored Procedures
  - Insert
  - Merge
  - Full Refresh
- Tasks
- Streams
- Snowpipes
- Types of Tables and views

## Recap of Existing Processes

**Amplitude Export API** 

- Python script to extract and upload data to S3 Bucket as JSON
- Snowflake Storage Integration to query raw data in Snowflake

## Bronze-to-Silver Medallion Pipeline aka Parsing Data into a Schema

### Bronze layer review

Our Bronze layer consists of one table with all the data stored as JSON. This will not be easy for data analysts, scientists or other engineers to work with, so for our silver layer, we will convert this into a schema of tables.  

```
{"$insert_id":"ea31ba7b-f543-4b9b-9aec-90fbaa96a355",
"$insert_key":null,"$schema":null,"adid":null,
"amplitude_attribution_ids":null,
"amplitude_event_type":null,
"amplitude_id":45975551903,
"app":100011471,
"city":"Hemel Hempstead",
"client_event_time":"2025-07-17 10:36:28.583000",
"client_upload_time":"2025-07-17 10:36:29.750000",
"country":"United Kingdom",
"data":{"path":"/2/httpapi",
"user_properties_updated":true,
"group_first_event":{},
"group_ids":{}},
"data_type":"event",
"device_brand":null,
"device_carrier":null,
"device_family":"Mac OS X",
"device_id":"4289c8b4-7afe-49ed-8814-51c5fc77c693",
"device_manufacturer":null,
"device_model":null,
"device_type":"Mac",
"dma":null,
"event_id":135,
"event_properties":{},
"event_time":"2025-07-17 10:36:28.583000",
"event_type":"session_start",
"global_user_properties":null,
"group_properties":{},
"groups":{},
"idfa":null,
"ip_address":"82.20.15.60",
"is_attribution_event":null,
"language":"English",
"library":"amplitude-ts/2.16.1",
"location_lat":null,
"location_lng":null,
"os_name":"Chrome",
"os_version":"137",
"partner_id":null,
"paying":null,
"plan":{},
"platform":"Web",
"processed_time":"2025-07-17 10:36:31.609000",
"region":"Hertfordshire",
"sample_rate":null,
"server_received_time":"2025-07-17 10:36:29.750000",
"server_upload_time":"2025-07-17 10:36:29.753000",
"session_id":1752748588583,
"source_id":null,
"start_version":null,
"user_creation_time":null,
"user_id":"jack@theinformationlab.co.uk",
"user_properties":{"initial_utm_medium":"EMPTY",
"initial_referring_domain":"EMPTY",
"initial_utm_content":"EMPTY",
"initial_utm_campaign":"EMPTY",
"initial_twclid":"EMPTY",
"initial_li_fat_id":"EMPTY",
"referrer":"https://www.google.com/",
"initial_gclid":"EMPTY",
"initial_utm_source":"EMPTY",
"initial_dclid":"EMPTY",
"initial_wbraid":"EMPTY",
"initial_fbclid":"EMPTY",
"initial_rtd_cid":"EMPTY",
"initial_utm_id":"EMPTY",
"initial_referrer":"EMPTY",
"initial_gbraid":"EMPTY",
"initial_utm_term":"EMPTY",
"initial_msclkid":"EMPTY",
"initial_ttclid":"EMPTY",
"initial_ko_click_id":"EMPTY",
"referring_domain":"www.google.com"},
"uuid":"3e702190-9dcb-483a-94d8-58e93ce161de",
"version_name":null}

```

Typically, the JSON structures can be parsed into a table using these selectors:

```
SELECT 
json_data:"$insert_id"::string as "$insert_id",
json_data:"$insert_key"::string as "$insert_key",
json_data:event_properties
FROM amplitude_events_raw;
```

This is a manual process, it is good to know what you can required to access fields from nested arrays as: 

```

SELECT
json_data:array_name:field_name::data_type as new_field_name
FROM table

```

Before we built our silver layer, it's important to plan it. Time to build our snowflake schema while taking into account the business requirements / questions: 
- What journeys are users taking on the website?
- Is a user making repeated clicks on the website? 
  - Could this indicate a problem with the website?
  - Can we flag any user behaviour that seems to indicate a problem with the website?
- Is there any evidence that a user finds the menu on the UK website confusing?
- Can we associate the IP address of a user with a particular company, so we can see which companies are visiting the website?

## Schema: 

<details>
    <summary>Solution Schema</summary>
<img width="1882" height="785" alt="{D03EB781-B10D-400E-BF09-012E467FAED1}" src="https://github.com/user-attachments/assets/751bf1aa-80eb-4259-8577-1f50ba02804d" />
</details>

## Create Tables: 


To create dimension tables you will need unique IDs to be generated, as seen in our plan! There are multiple options to create unique IDs, you can either use a function like HASH() or MD5(), or create your dimension tables before the facts table, using a rowID for each distinct row and joining that dimension table to other dimension tables and to the raw data to create your facts table. 

### Option 1: HASH

The HASH() function will create an integer which is always assigned to the value of the field(s) you tell it to be based on, for example hashing "Belgium" leads to a HASH of -5115476029316419222. You can HASH multiple fields, hashing "Belgium" and "London" leads to a HASH of 8345500420073462720. Be careful, the order of the fields you are hashing matters. 

```
SELECT HASH(col1, col2, col3) as event_id
  , col1
  , col2
  , col3
FROM raw_data
```

This is easy to set up, now everytime a combination of col1, col2, and col3, is hashed you will get the same value, meaning you can use this to create both your dimensions and fact tables. 
HASH will be a 64bit integer, this is very fast, not much storage but it always recomputes. This means over time, if you refresh the table, compute costs will accumulate. A big pro is that you also don't need to maintain it, your dimensions will always have the same hash and therefore, the same ID. I believe most clients will accept this, as the compute costs are a pretty negligeable part of what they are ready to spend on their data platforms.

### Option 2: Dimension table + join

```
-- first build a dimension table
CREATE OR REPLACE TABLE dimensions AS
SELECT
  ROW_NUMBER() OVER (Order BY col1, col2, col3) as event_id
  , col1
  , col2
  , col3
FROM (
  Select DISTINCT col1, col2, col3 FROM raw_data);

-- Then build fact table
SELECT r.*
  , d.event_id
  , r.col1
  , r.col2
  , r.col3
from r.raw_data
JOIN d.dimensions
ON r.col1 = d.col1
AND r.col2 = d.col2
AND r.col3 = d.col3
```

This method is more complicated and manually heavy. It requires more maintenance and it probably more error prone because new values entering the dimensions table could lead to IDs changing unless you can always insert them at the bottom of the table with a new ID. Joins are very effective in Snowflake, your join IDs can be smaller therefore less storage and the computing power is less as joins scale better than hash when having mulitple clauses.

### Let's build the tables!
Split the group into 4, a few tables each, then share your tables with other groups.

### But how do we update those tables? Procedures!

## What is a stored procedure? 

https://docs.snowflake.com/en/developer-guide/stored-procedure/stored-procedures-overview

You can write stored procedures to extend the system with procedural code. With a procedure, you can use branching, looping, and other programmatic constructs. You can reuse a procedure multiple times by calling it from other code.

A Snowflake stored procedure code is wrapped in a function taking the snowpark_session and any arguments you have given as options. The snowpark_session will give you access to execute SQL queries through Python.

Can you Refresh the City table using a procedure? 

Calling the procedure will refresh our action table! So remember to test it!

<details>
    <summary>Solution Procedure Amplitude City</summary>
  
```
-- Creating a Procedure : 
CREATE OR REPLACE PROCEDURE REFRESH_S_AMPLITUDE_CITY()
returns varchar
language sql
as
$$
CREATE OR REPLACE TABLE S_AMPLITUDE_CITY AS
select distinct
"city" as city_name,
hash("city") as city_id
from B_amplitude_events;
$$
;

--Run procedure
CALL REFRESH_S_AMPLITUDE_CITY();
```
  </details>


# Insert statements! 

https://docs.snowflake.com/en/sql-reference/sql/insert

You can use the insert statement to filter out the data coming into. While Copy Into uses metadata to not copy your files twice into a stage, procedures will not, meaning you need to filter our the data you introduce. Metadata or a last refresh date is very useful for this purpose. Insert Into can also be a more efficient version of fully refreshing a table if using the Overwrite parameter. This is the equivalent of truncating the table and inserting data back in. This is supported by Snowflake, Databricks and Bigquery (different syntax), but not by SQLServer, MySQL or Oracle. 

Can you use a procedure to only insert new rows into the country silver layer table? 

<details>
    <summary>Solution Insert Statement Country </summary>

```
CREATE OR REPLACE PROCEDURE REFRESH_S_AMPLITUDE_COUNTRY() 
returns varchar
language sql
as
$$
BEGIN --Remember Begin and End so you can have multiple queries within your procedures 

INSERT INTO S_AMPLITUDE_COUNTRY
select distinct
"country" as country_name,
hash("country") as country_id
from amplitude_events e
JOIN AMPLITUDE_EXTRACT_MAX m --Remember You need to have created a table with your max extract first! 
WHERE e."_airbyte_extracted_at" > m.max_extract; 

INSERT OVERWRITE INTO AMPLITUDE_EXTRACT_MAX
SELECT MAX("_airbyte_extracted_at")
FROM amplitude_events;
END
;
$$;
```
  </details>

    
# Merge Statements! 

https://docs.snowflake.com/en/sql-reference/sql/merge

You can also use a merge, this will allow you to both insert new rows in the target table while updating or deleting values in your target values if they have changed or don't exist anymore. 
This is very heavy to compute as you have to go through every single row of the data. 

Using the documentation, can you use a merge statement to refresh the ampltidue events silver layer table? 

<details>
    <summary> Solution for the merge of Amplitude Events </summary>

```
CREATE OR REPLACE PROCEDURE REFRESH_S_AMPLITUDE_EVENTS()
returns varchar
language sql
as
$$
MERGE INTO S_AMPLITUDE_EVENTS tgt
USING (
    SELECT DISTINCT
         eve."uuid"               AS event_id,
         eve."session_id"         AS session_id,
         eve."event_id"           AS session_event_order,
         evel."id"                AS id,
         eve."event_time"         AS event_time,
         HASH(eve."user_properties") AS user_properties_id,
         HASH(eve."event_properties") AS event_properties_id
    FROM B_amplitude_events eve
    LEFT JOIN B_AMPLITUDE_EVENTS_LISTS evel
        ON eve."event_type" = evel."name"
    WHERE eve."event_time" > (
        SELECT COALESCE(MAX(event_time), '1900-01-01') FROM S_AMPLITUDE_EVENTS ) 
) src
ON tgt.event_id = src.event_id
WHEN NOT MATCHED THEN INSERT (
    event_id,
    session_id,
    session_event_order,
    events_list_id,
    event_time,
    user_properties_id,
    event_properties_id
) VALUES (
    src.event_id,
    src.session_id,
    src.session_event_order,
    src.id,
    src.event_time,
    src.user_properties_id,
    src.event_properties_id
);  
$$
;
```
  </details>

Remember this is heavy in computing power, I would only do this if I know my facts table isn't too big and that rows will often need to be updated. 

# Scheduling procedures with tasks and streams!

Tasks allow you to automate data processing. They can run at scheduled times or be triggered by events, such as when new dat arrives in a stream. 
A stream is an object that records data manipulation language changes made to tables, this includes inserts, copy into, and any metadata changes. Once those actions are recordes you can use the stream as a change data capture process to trigger queries and procedures. The inserted or updated rows will be stored in the stream.  

```
-- creating stream 
CREATE OR REPLACE STREAM Amplitude_raw ON TABLE B_AMPLITUDE_EVENTS;

-- Currently Stream is empty 
SELECT 1 FROM AMPLITUDE_RAW;

-- inserting 2 rows via duplication
INSERT INTO B_AMPLITUDE_EVENTS ( 
SELECT * FROM B_AMPLITUDE_EVENTS 
limit 2);

-- now there's 2 rows here! 
SELECT 1 FROM AMPLITUDE_RAW;

-- Create a task triggered by the stream
CREATE OR REPLACE TASK TRIGGER_S_AMPLITUDE_EVENTS_REFRESH
WAREHOUSE = dataschool_wh
SCHEDULE = '5 MINUTE' --this means every 5min, the task will check if the stream has data. 
WHEN SYSTEM$STREAM_HAS_DATA('Amplitude_raw')
as
CALL REFRESH_S_AMPLITUDE_EVENTS();

-- Now after resuming the task
-- now there's 2 rows here! 
SELECT 1 FROM AMPLITUDE_RAW;

--Check task has run
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY());

-- check stream for data, still has some! 
SELECT 1 FROM AMPLITUDE_RAW;

```
Be careful, the task will only run once resumed, it is by default suspended. For the stream to offset data, it needs to be used in the procedure! Instead of using the latest refresh table we created, we would use the stream directly. 

Now this means our stream will look for data every 5 minutes... which is pretty often, and if we don't load data into our stage long enough will just cost us a lot for nothing. You can either change that to be less often but then maybe we can skip the stream all together and just schedule a task based on time: 

```
-- Daily Task for country 
CREATE TASK DAILY_S_AMPLITUDE_COUNTRY
WAREHOUSE = dataschool_wh
  SCHEDULE='USING CRON 0 8 * * * Europe/London'
AS
CALL REFRESH_S_AMPLITUDE_COUNTRY();

```
After creating a task you need to make sure it is resumed, this can be done through the UI or via this line: 

```

ALTER TASK task_name RESUME;

```

There is also the possibility of running cxhains of task by running them one by one: 

```
-- task following 
CREATE OR REPLACE TASK DAILY_G_AMPLITUDE
WAREHOUSE = dataschool_wh
  AFTER DAILY_S_AMPLITUDE_COUNTRY
AS
SELECT * FROM S_AMPLITUDE_COUNTRY; 
```

Remember that a task running is computing which means it has a cost. So please pause your streams and tasks after use. 

# Snow pipes $$

A Snowpipe is a continuous data ingestion tool, it will constantly scans the stage and when there are new files found, it triggers a copy into commant which inserts the new data into the target table. Snowflake uses metadata to not load data from the same file twice. This is very expensive, only to use if you need a really fast data ingestion. I doubt many clients requires them. 

```
CREATE OR REPLACE PIPE Amplitude_Snowpipe
auto_ingest = TRUE
as 
COPY INTO B_AMPLITUDE_EVENTS
FROM @TIL_DATA_ENGINEERING.JC_DENG_3_STAGING.AMPLITUDE_AIRBYTE_ST/events/
FILE_FORMAT = TIL_DATA_ENGINEERING.JC_DENG_3_STAGING.JC_JSON_FORMAT
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';

-- This makes sure old files are uploaded from your stage to your table!
ALTER PIPE Amplitude_Snowpipe REFRESH; 
```

This is one where I would ask for permission before building and make sure the client is aware of the costs. 

# Dynamic Tables

Dynamic Tables

Also pricy, dynamic tables automatically refresh based on a defined query. Essentially, they will refresh whenever any base tables (tables used in the query which builds the dynamic table). If your base tables refresh often, then so will your dyanmic tables. You set up a lag in dynamic tables which is how often it will refresh if the base tables have not changed. 

<img width="1180" height="1111" alt="image" src="https://github.com/user-attachments/assets/778f957a-fa19-436b-af0e-aa3fe9e89d21" />

```
-- Create dynamic Table
CREATE OR REPLACE DYNAMIC TABLE G_AMPLITUDE_LOCATION
LAG = '1 MINUTE'
WAREHOUSE = DATASCHOOL_WH
as
SELECT LOCATION_ID, CITY_NAME, COUNTRY_NAME, IP_ADDRESS FROM S_AMPLITUDE_LOCATION L
JOIN S_AMPLITUDE_CITY C
ON C.CITY_ID = L.CITY_ID
JOIN S_AMPLITUDE_COUNTRY D
ON D.COUNTRY_ID = L.COUNTRY_ID;

-- checking table
SELECT COUNT(*) FROM G_AMPLITUDE_LOCATION; 

-- inserting 2 rows via duplication
INSERT INTO S_AMPLITUDE_LOCATION ( 
SELECT * FROM S_AMPLITUDE_LOCATION 
limit 2);

-- one min later...

-- checking table
SELECT COUNT(*) FROM G_AMPLITUDE_LOCATION; 
```



## Appendix

### References

Snowflake Storage Integration

- S3: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
- Azure: https://docs.snowflake.com/en/user-guide/data-load-azure-config

Snowpipe
- Snowflake: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro
- CREATE PIPE: https://docs.snowflake.com/en/sql-reference/sql/create-pipe
- Amazon S3: https://docs.aws.amazon.com/AmazonS3/latest/userguide/enable-event-notifications.html

  
Snowflake Stored Procedures: https://docs.snowflake.com/en/developer-guide/stored-procedure/stored-procedures-overview

Tasks in Snowflake: https://docs.snowflake.com/en/user-guide/tasks-intro

