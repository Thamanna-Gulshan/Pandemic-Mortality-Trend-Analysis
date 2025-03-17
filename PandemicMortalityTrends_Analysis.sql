/* DATABASE CREATION */

CREATE DATABASE pandemic; 

USE pandemic;

/* CREATING TABLE SCHEMA */

CREATE TABLE covid 
(data_as_of varchar(500), 
Jurisdiction_Residence varchar(500),	
`Group` varchar(500),	
data_period_start varchar(500),	
data_period_end	varchar(500),
COVID_deaths varchar(500),
COVID_pct_of_total varchar(500),
pct_change_wk varchar(500),
pct_diff_wk	varchar(500),
crude_COVID_rate varchar(500),
aa_COVID_rate varchar(500));

/* IMPORTING DATA */

-- Enable the local_infile parameter to permit local data loading 
SHOW VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1; 

-- Load data
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\DA_Data.csv'
INTO TABLE covid
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

SELECT * FROM covid;

/* DATA CLEANING */

SELECT Column_name, Data_type 
FROM information_schema.columns
WHERE table_Name='covid';

/* Correcting data type of columns */

-- Formatting date values to supported date format

UPDATE covid SET 
data_as_of =  STR_TO_DATE(data_as_of, '%d-%m-%Y'),
data_period_start = 
CASE WHEN data_period_start LIKE '%-%-%' THEN STR_TO_DATE(data_period_start, '%m-%d-%Y') 
WHEN data_period_start LIKE '%/%' THEN STR_TO_DATE(data_period_start, '%m/%d/%Y') END,
data_period_end = 
CASE WHEN data_period_end LIKE '%-%-%' THEN STR_TO_DATE(data_period_end, '%m-%d-%Y')
WHEN data_period_end LIKE '%/%' THEN STR_TO_DATE(data_period_end, '%m/%d/%Y') END;

-- Updating missing values to NULL values
UPDATE covid SET 
pct_change_wk = IF(pct_change_wk = '', NULL, pct_change_wk),
pct_diff_wk = IF(pct_diff_wk = '', NULL, pct_diff_wk),
COVID_deaths = IF(COVID_deaths = '', NULL, COVID_deaths),
COVID_pct_of_total = IF(COVID_pct_of_total = '', NULL, COVID_pct_of_total),
crude_COVID_rate = IF(crude_COVID_rate = '', NULL, crude_COVID_rate),
aa_COVID_rate = IF(TRIM(aa_COVID_rate) = '', NULL, aa_COVID_rate);

UPDATE covid SET aa_COVID_rate = NULL
WHERE TRIM(REPLACE(REPLACE(aa_COVID_rate, '\r', ''), '\n', '')) = '' OR aa_COVID_rate IS NULL;
      
-- Modifying the data types of the columns
ALTER TABLE covid
MODIFY COLUMN data_as_of DATE,
MODIFY COLUMN data_period_start DATE,
MODIFY COLUMN data_period_end DATE,
MODIFY COLUMN COVID_deaths INT,
MODIFY COLUMN COVID_pct_of_total DOUBLE,
MODIFY COLUMN pct_change_wk DOUBLE,
MODIFY COLUMN pct_diff_wk DOUBLE,
MODIFY COLUMN crude_COVID_rate DOUBLE,
MODIFY COLUMN aa_COVID_rate DOUBLE;


-- Identifying duplicate records
WITH duplicate_cte AS
(SELECT *, ROW_NUMBER() OVER(PARTITION BY data_as_of, Jurisdiction_Residence, `Group`, data_period_start, data_period_end) AS ranking
FROM covid)
SELECT * FROM duplicate_cte WHERE ranking>1;
-- No duplicate records are present

-- Identifying missing values in primary columns
SELECT * 
FROM covid
WHERE COALESCE(data_as_of, '') = '' 
   OR COALESCE(Jurisdiction_Residence, '') = '' 
   OR COALESCE(`Group`, '') = '' 
   OR COALESCE(data_period_start, '') = '' 
   OR COALESCE(data_period_end, '') = '';

/* ABOUT DATA */

SELECT distinct data_as_of FROM covid;
-- The data is collected on -> 2023-12-04

SELECT distinct Jurisdiction_Residence FROM covid;
-- Data is collected across 64 regions

SELECT distinct `group` FROM covid;
-- Data is divided on total, weekly and 3 month period basis

SELECT MAX(data_period_end) AS latest_date FROM covid;
-- The latest data period end date is 2023-04-08

SELECT Jurisdiction_Residence, MIN(data_period_start), MAX(data_period_end) FROM covid GROUP by Jurisdiction_Residence; 
-- The overall data for each Jurisdiction_Residence is collected for the period -> 2019-12-29 to 2023-04-08

/* ANALYSIS */

SELECT * FROM covid;

/* Creating view for the total data with the latest data_period_end */
CREATE VIEW total_data AS
SELECT * 
FROM covid
WHERE `group` = 'total' AND
data_period_end = (SELECT MAX(data_period_end) FROM covid);


/* Retrieve the jurisdiction residence with the highest number of COVID deaths for the latest data period end date. */

SELECT data_period_start, data_period_end, Jurisdiction_Residence,COVID_deaths 
FROM (SELECT data_period_start, data_period_end, Jurisdiction_Residence, COVID_deaths, 
RANK() OVER(ORDER BY COVID_deaths DESC) AS ranking
FROM total_data)A
WHERE ranking=1;

/* 
Retrieve the top 5 jurisdictions with the highest percentage difference in aa_COVID_rate  
compared to the overall crude COVID rate for the latest data period end date.
 */
  
WITH overall_crude_rate AS 
(SELECT AVG(crude_COVID_rate) AS overall_crude_rate FROM total_data),

jurisdictions_with_diff AS (
SELECT 
c.data_period_end,
c.Jurisdiction_Residence,
aa_COVID_rate,
crude_Covid_rate,
ROUND(((c.aa_COVID_rate - ocr.overall_crude_rate) / ocr.overall_crude_rate) * 100,2) AS percent_difference
FROM total_data c
CROSS JOIN overall_crude_rate ocr
)
SELECT *
FROM jurisdictions_with_diff
ORDER BY percent_difference DESC
LIMIT 5;


/* Calculate the average COVID deaths per week for each jurisdiction residence and group, 
for the latest 4 data period end dates.*/

WITH latest_dates AS (
SELECT distinct data_period_end
FROM covid
ORDER BY data_period_end DESC
LIMIT 4
)
SELECT Jurisdiction_Residence, ROUND(AVG(COVID_deaths),2) AS average_deaths_per_week
FROM covid c
JOIN latest_dates d on c.data_period_end = d.data_period_end
WHERE c.`Group` = 'weekly'
GROUP BY c.Jurisdiction_Residence
ORDER BY average_deaths_per_week DESC;

/* Retrieve the data for the latest data period end date, 
but exclude any jurisdictions that had zero COVID deaths and have missing values in any other column.*/

SELECT *
FROM covid 
WHERE data_period_end = (SELECT MAX(data_period_end) FROM covid) AND
COVID_deaths > 0 
AND 
(COVID_pct_of_total IS NOT NULL AND TRIM(COVID_pct_of_total) !='' 
AND pct_change_wk IS NOT NULL AND TRIM(pct_change_wk) != '' 
AND pct_diff_wk IS NOT NULL AND TRIM(pct_diff_wk) != '' 
AND crude_COVID_rate IS NOT NULL AND TRIM(crude_COVID_rate) != '' 
AND aa_COVID_rate IS NOT NULL AND TRIM(aa_COVID_rate) != '');

/* Calculate the week-over-week percentage change in COVID_pct_of_total for all jurisdictions and groups, 
but only for the data period start dates after March 1, 2020.*/

WITH filtered_data AS (
SELECT 
data_period_start,data_period_end,
Jurisdiction_Residence,
`Group`,
COVID_pct_of_total
FROM covid
WHERE data_period_start > '2020-03-01' AND 
`Group` = 'weekly'
),
week_over_week_change AS (
SELECT *,
LAG(f.COVID_pct_of_total) OVER (PARTITION BY f.Jurisdiction_Residence, f.`Group` ORDER BY f.data_period_start) AS prev_week_pct
FROM filtered_data f
)
SELECT *,
ROUND(((COVID_pct_of_total - prev_week_pct) / prev_week_pct) * 100,2) AS week_over_week_pct_change
FROM week_over_week_change
ORDER BY Jurisdiction_Residence, `Group`, data_period_start;


/* Group the data by jurisdiction residence and calculate the cumulative COVID deaths for each  jurisdiction, 
but only up to the latest data period end date. */

SELECT 
    Jurisdiction_Residence,
    data_period_end,
    COVID_deaths,
    SUM(COVID_deaths) OVER (PARTITION BY Jurisdiction_Residence ORDER BY data_period_end) AS cumulative_covid_deaths
FROM covid
WHERE `group` = 'total'
ORDER BY Jurisdiction_Residence,data_period_end;

/* Create a stored procedure 
that takes in a date range 
and calculates the average weekly percentage change in COVID deaths for each  jurisdiction. 
The procedure should return the average weekly percentage change along with the jurisdiction and date range as output. */

DELIMITER |
CREATE PROCEDURE CalculateAvgWeeklyPctChange(
    IN start_date DATE, 
    IN end_date DATE
)
BEGIN
-- Select the average weekly percentage change for each jurisdiction in the given date range
SELECT 
Jurisdiction_Residence,
start_date AS start_range_date,
end_date end_range_date,
ROUND(AVG(pct_change_wk),2) AS avg_weekly_pct_change
FROM covid
WHERE data_period_start BETWEEN start_date AND end_date
GROUP BY Jurisdiction_Residence;
END|

-- Calling the procedure
CALL CalculateAvgWeeklyPctChange('2020-03-22','2020-04-04')

/* create a user-defined function 
that takes in a jurisdiction as input and 
returns the average crude COVID rate for that jurisdiction over the entire dataset. */

DELIMITER |
CREATE FUNCTION calculateAvgCrudeRate(Residence varchar(20)) RETURNS DOUBLE
DETERMINISTIC
BEGIN 
DECLARE avg_crude_rate DOUBLE;
SELECT ROUND(AVG(crude_COVID_rate),2) INTO avg_crude_rate
FROM covid
WHERE Jurisdiction_Residence = Residence;
RETURN avg_crude_rate;
END|

-- Calling the function
SELECT calculateAvgCrudeRate('Region 1') AS avg_crude_rate;