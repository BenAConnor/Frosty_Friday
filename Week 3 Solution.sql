/*Create csv format for files to be loaded in as, added skip header parameter
to avoid column headers being loaded as data*/
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1;

/*Created S3 stage and specified csv format*/
CREATE OR REPLACE STAGE WEEK3_S3
URL = 's3://frostyfridaychallenges/challenge_3/'
FILE_FORMAT = 'CSV_FORMAT';

/*list to check what is in the S3 bucket*/
list @WEEK3_S3;

/*Created empty table with columns from csv files ready to load data into*/
CREATE OR REPLACE TABLE WEEK_3_Table (
id TEXT,
first_name TEXT,
last_name TEXT,
catch_phrase TEXT,
timestamp TEXT
);

/*Copy data from stage in new table*/
COPY INTO WEEK_3_Table
FROM @WEEK3_S3
ON_ERROR = 'skip_file'
;

/*View table to check it has worked properly*/
SELECT * FROM WEEK_3_Table;

/*Create table with csv filenames attached from the same S3 bucket (dont think this is efficient)*/
CREATE OR REPLACE TABLE WEEK_3_FILENAMES AS
SELECT METADATA$FILENAME, $1 AS id, $2 AS FIRST_NAME, $3 AS LAST_NAME, $4 AS CATCH_PHRASE, $5 AS TIMESTAMP
FROM @WEEK3_S3;

/*This query seems very cumbersome but does work - create new table joining the unioned csv files onto
the table containing the file names, have to join on every column to avoid duplication, filter to the relevant 
key words of the file names, group by filename and count the records*/
CREATE OR REPLACE TABLE WEEK3_SOLUTION AS (
    WITH CTE AS (
        SELECT *, t2.id, METADATA$FILENAME AS FILENAME, t2.FIRST_NAME,t2.CATCH_PHRASE,t2.TIMESTAMP
        FROM WEEK_3_Table AS t1
            INNER JOIN WEEK_3_FILENAMES AS t2
                ON t1.id = t2.id AND
                t1.FIRST_NAME = t2.FIRST_NAME AND
                t1.LAST_NAME = t2.LAST_NAME AND
                t1.CATCH_PHRASE = t2.CATCH_PHRASE AND
                t1.TIMESTAMP = t2.TIMESTAMP
        WHERE CONTAINS(FILENAME,'forgot')
        OR CONTAINS(FILENAME,'extra')
        OR CONTAINS(FILENAME,'added')
    )
    SELECT COUNT(FILENAME) AS NUMBER_OF_ROWS, FILENAME
    FROM CTE
    GROUP BY FILENAME
);
/*Query table to check results match with the solution*/
SELECT * FROM WEEK3_SOLUTION