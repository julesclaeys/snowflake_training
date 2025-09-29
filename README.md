
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

Split the group into 4, a few tables each. How will you build unique IDs for all your dimensions? 

There are multiple options to create unique IDs, you can either use a function like HASH() or MD5(), or create your dimension tables before the facts table, using a rowID for each distinct row and joining that dimension table to other dimension tables and to the raw data to create your facts table. 

#Option 1: HASH

```
SELECT HASH(col1, col2, col3) as event_id
  , col1
  , col2
  , col3
FROM raw_data
```

This is easy to set up, now everytime a combination of col1, col2, and col3, is hashed you will get the same value, meaning you can use this to create both your dimensions and fact tables. 
HASH will be a 64bit integer, this is very fast, not much storage but it always recomputes. This means over time,  


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

