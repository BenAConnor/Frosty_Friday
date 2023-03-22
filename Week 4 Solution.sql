/*Create JSON file format*/
CREATE OR REPLACE FILE FORMAT JSON_FORMAT
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE;

/*Create S3 external stage*/
CREATE OR REPLACE STAGE WEEK4_Stage
URL = 's3://frostyfridaychallenges/challenge_4'
FILE_FORMAT = 'JSON_FORMAT';

/*Check what is in the S3 Bucket*/
list @WEEK4_Stage;

/*Create empty table with a 'variant' data type to copy the json into*/
CREATE OR REPLACE TABLE raw_json
(
SRC variant
)
;

/*Copy json into above table from the stage*/
COPY INTO raw_json
  FROM @WEEK4_Stage
  FILE_FORMAT = 'JSON_FORMAT';

/*Creating columns for all the key/value pairs in the json array and adding relevant
index columns. Have to use IFNULL for the Nickname and Consort fields as M.Value:Nickname[0]
will return null if a Monarch only has one nickname because the single value will not be in a seperate array*/

CREATE OR REPLACE TABLE WEEK4_MONARCHS_SOLUTION AS
SELECT 
        ROW_NUMBER() OVER(ORDER BY M.value:Birth) AS ID,
        M.INDEX + 1 AS Inter_House_ID,
        SRC:Era::string AS Era,
        H.value:House::string AS HOUSES,
        M.value:Name::string AS Monarch,
        IFNULL(M.value:Nickname[0]::string,M.value:Nickname::string) AS Nickname_1,
        M.value:Nickname[1]::string AS Nickname_2,
        M.value:Nickname[2]::string AS Nickname_3,
        M.value:Birth::string AS Birth,
        M.value:"Start of Reign"::date AS Start_of_Reign,
        M.value:"End of Reign"::date AS End_of_Reign,
        M.value:Duration::string AS Duration,
        M.value:Death::string AS Death,
        IFNULL(M.value:"Consort\/Queen Consort"[0],M.value:"Consort\/Queen Consort")::string AS Consort_Or_Queen_Consort_1,
        M.value:"Consort\/Queen Consort"[1]::string AS Consort_Or_Queen_Consort_2,
        M.value:"Place of Birth"::string AS Place_of_Birth,
        M.value:"Place of Death"::string AS Place_of_Death,
        M.value:"Age at Time of Death"::string AS Age_at_Time_of_Death,
        M.value:"Burial Place"::string AS Burial_Place
        
--Flatten inner levels of json array (naming sections H and M)  
    FROM raw_json
    , LATERAL FLATTEN(INPUT => SRC:Houses)H
    , LATERAL FLATTEN(INPUT => H.value:Monarchs)M ;