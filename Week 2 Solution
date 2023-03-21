
CREATE OR REPLACE FILE FORMAT PARQUET
    TYPE = parquet;


CREATE OR REPLACE STAGE WEEK2_PARQUET_STAGE
    URL = 's3://frostyfridaychallenges/challenge_2';


list @WEEK2_PARQUET_STAGE;


SELECT * 
FROM @WEEK2_PARQUET_STAGE (
    PATTERN => 'challenge_2/employees.parquet',
    FILE_FORMAT => 'PARQUET'
    );


CREATE OR REPLACE TABLE WEEK2_SOLUTION CHANGE_TRACKING = TRUE
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE (
            INFER_SCHEMA(
            LOCATION => '@WEEK2_PARQUET_STAGE',
            FILE_FORMAT => 'PARQUET'
            )
    ));

COPY INTO WEEK2_SOLUTION
FROM @WEEK2_PARQUET_STAGE
    PATTERN = 'challenge_2/employees.parquet'
    FILE_FORMAT = 'PARQUET'
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    PURGE = FALSE;


CREATE OR REPLACE VIEW WEEK2_SOLUTION_VIEW AS
    SELECT "employee_id",
    "job_title",
    "dept",
    "last_name",
    "country",
    "title"
FROM WEEK2_SOLUTION;


CREATE OR REPLACE STREAM WEEK2_STREAM ON VIEW WEEK2_SOLUTION_VIEW;


UPDATE WEEK2_SOLUTION SET "country" = 'Japan' WHERE "employee_id" = 8;
UPDATE WEEK2_SOLUTION SET "last_name" = 'Forester' WHERE "employee_id" = 22;
UPDATE WEEK2_SOLUTION SET "dept" = 'Marketing' WHERE "employee_id" = 25;
UPDATE WEEK2_SOLUTION SET "title" = 'Ms' WHERE "employee_id" = 32;
UPDATE WEEK2_SOLUTION SET "job_title" = 'Senior Financial Analyst' WHERE "employee_id" = 68;


SELECT * FROM WEEK2_STREAM
