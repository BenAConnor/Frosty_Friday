--Creating the python function in snowflake (multiple a number by 3)

CREATE OR REPLACE FUNCTION Multiplyby3(i int)
RETURNS int
LANGUAGE python
runtime_version = '3.8'
handler = 'multiplyby3_py'
AS
$$
def multiplyby3_py(i):
    return i*3
$$;

--Create a table to test the function on with an integer value
CREATE OR REPLACE TABLE WEEK5_TEST_TABLE(
NUMBER INT 
);

INSERT INTO WEEK5_TEST_TABLE 
("NUMBER")
VALUES (7);

--Check table before function applied
SELECT * FROM WEEK5_TEST_TABLE ;

--Check table after applying python function
SELECT Multiplyby3("NUMBER") AS SOLUTION
FROM WEEK5_TEST_TABLE ;