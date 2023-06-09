/*Creating Parquet file format to load data*/
CREATE OR REPLACE FILE FORMAT PARQUET
    TYPE = parquet;

/*Create an external stage linking to S3 URL*/
CREATE OR REPLACE STAGE WEEK2_PARQUET_STAGE
    URL = 's3://frostyfridaychallenges/challenge_2';

/*Checking what iss in the S3 bucket*/
list @WEEK2_PARQUET_STAGE;

/*Selecting the parquet file from the S3 bucket to ensure it loads correctly*/
SELECT * 
FROM @WEEK2_PARQUET_STAGE (
    PATTERN => 'challenge_2/employees.parquet',
    FILE_FORMAT => 'PARQUET'
    );

/*Creating table for the solution (enable change tracking)
This creates the table using the schema of the file (only possible with parquet)*/
CREATE OR REPLACE TABLE WEEK2_SOLUTION CHANGE_TRACKING = TRUE
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE (
            INFER_SCHEMA(
            LOCATION => '@WEEK2_PARQUET_STAGE',
            FILE_FORMAT => 'PARQUET'
            )
    ));

/*Copy data from stage into the new table*/
COPY INTO WEEK2_SOLUTION
FROM @WEEK2_PARQUET_STAGE
    PATTERN = 'challenge_2/employees.parquet'
    FILE_FORMAT = 'PARQUET'
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    PURGE = FALSE;

/*Create a view based on that table*/
CREATE OR REPLACE VIEW WEEK2_SOLUTION_VIEW CHANGE_TRACKING = TRUE AS
    SELECT "employee_id",
    "dept",
    "job_title"   
FROM WEEK2_SOLUTION;

/*Create a stream that will track any changes to this view*/
CREATE OR REPLACE STREAM WEEK2_STREAM ON VIEW WEEK2_SOLUTION_VIEW;

/*Edit table with the supplied instructions*/
UPDATE WEEK2_SOLUTION SET "country" = 'Japan' WHERE "employee_id" = 8;
UPDATE WEEK2_SOLUTION SET "last_name" = 'Forester' WHERE "employee_id" = 22;
UPDATE WEEK2_SOLUTION SET "dept" = 'Marketing' WHERE "employee_id" = 25;
UPDATE WEEK2_SOLUTION SET "title" = 'Ms' WHERE "employee_id" = 32;
UPDATE WEEK2_SOLUTION SET "job_title" = 'Senior Financial Analyst' WHERE "employee_id" = 68;

/*Check the stream to see historical changes*/
SELECT * FROM WEEK2_STREAM
