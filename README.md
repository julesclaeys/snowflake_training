
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
