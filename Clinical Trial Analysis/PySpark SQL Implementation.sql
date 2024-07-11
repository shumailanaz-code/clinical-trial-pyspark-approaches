-- Databricks notebook source
-- MAGIC %md
-- MAGIC Database Creation, Cleaning and Preparation

-- COMMAND ----------

--- Database Creation

CREATE DATABASE IF NOT EXISTS ct_db


-- COMMAND ----------

--- Viewing the Database

SHOW DATABASES


-- COMMAND ----------

--- Combining all Columns together for clinicaltrial_2023

CREATE OR REPLACE TEMPORARY VIEW merged_view AS
SELECT CONCAT_WS(' ', *) AS merged_column
FROM default.clinicaltrial_2023;


-- COMMAND ----------

--- Splitting merged_column into multiple Columns for clinicaltrial_2023

CREATE OR REPLACE TEMPORARY VIEW split_view AS
SELECT 
    SPLIT(merged_column, '\t')[0] AS Id,
    SPLIT(merged_column, '\t')[1] AS StudyTitle,
    SPLIT(merged_column, '\t')[2] AS Acronym,
    SPLIT(merged_column, '\t')[3] AS Status,
    SPLIT(merged_column, '\t')[4] AS Conditions,
    SPLIT(merged_column, '\t')[5] AS Interventions,
    SPLIT(merged_column, '\t')[6] AS Sponsor,
    SPLIT(merged_column, '\t')[7] AS Collaborators,
    SPLIT(merged_column, '\t')[8] AS Enrollment,
    SPLIT(merged_column, '\t')[9] AS FunderType,
    SPLIT(merged_column, '\t')[10] AS Type,
    SPLIT(merged_column, '\t')[11] AS StudyDesign,
    SPLIT(merged_column, '\t')[12] AS Start,
    SPLIT(merged_column, '\t')[13] AS Completion 
FROM merged_view;


-- COMMAND ----------

--- Create Table for clinicaltrial_2023

CREATE OR REPLACE TABLE ct_db.clinicaltrial_2023 AS SELECT * FROM split_view


-- COMMAND ----------

--- Create Table for clinicaltrial_2021

CREATE OR REPLACE TABLE ct_db.clinicaltrial_2021 AS SELECT * FROM default.clinicaltrial_2021


-- COMMAND ----------

--- Create Table for clinicaltrial_2020

CREATE OR REPLACE TABLE ct_db.clinicaltrial_2020 AS SELECT * FROM default.clinicaltrial_2020


-- COMMAND ----------

--- Create Table for pharma

CREATE OR REPLACE TABLE ct_db.pharma AS SELECT * FROM default.pharma


-- COMMAND ----------

--- View Tables Created in Database

SHOW TABLES IN ct_db


-- COMMAND ----------

--- View Few Records of clinicaltrial_2023 Table

SELECT * FROM ct_db.clinicaltrial_2023


-- COMMAND ----------

--- View Few Records of clinicaltrial_2021 Table

SELECT * FROM ct_db.clinicaltrial_2021


-- COMMAND ----------

--- View Few Records of clinicaltrial_2020 Table

SELECT * FROM ct_db.clinicaltrial_2020


-- COMMAND ----------

--- View Few Records of pharma Table

SELECT * FROM ct_db.pharma


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Q1: The Number of Distinct Studies in the Dataset

-- COMMAND ----------

--- The Number of Studies in clinicaltrial_2023

SELECT COUNT(DISTINCT Id) AS NumberOfStudies2023 
FROM ct_db.clinicaltrial_2023


-- COMMAND ----------

--- The Number of Studies in clinicaltrial_2021

SELECT COUNT(DISTINCT Id) AS NumberOfStudies2021 
FROM ct_db.clinicaltrial_2021


-- COMMAND ----------

--- The Number of Studies in clinicaltrial_2020

SELECT COUNT(DISTINCT Id) AS NumberOfStudies2020 
FROM ct_db.clinicaltrial_2020


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Q2: List all Types of Studies in the dataset along with the frequencies of each type and ordered them from most frequent to least frequent

-- COMMAND ----------

--- Types of Studies for clinicaltrial_2023

SELECT DISTINCT(Type), COUNT(Type) AS Frequencies 
FROM ct_db.clinicaltrial_2023
WHERE Type IS NOT NULL AND LENGTH(Type) > 0
GROUP BY Type
ORDER BY Frequencies DESC


-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC ### Visualization of Types of Studies for clinicaltrial_2023
-- MAGIC
-- MAGIC import pandas as pd
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC
-- MAGIC # Execute SQL query and Store the result in a pandas DataFrame
-- MAGIC
-- MAGIC sql_query_result = spark.sql("""
-- MAGIC SELECT DISTINCT(Type), COUNT(Type) AS Frequencies 
-- MAGIC FROM ct_db.clinicaltrial_2023
-- MAGIC WHERE Type IS NOT NULL AND LENGTH(Type) > 0
-- MAGIC GROUP BY Type
-- MAGIC ORDER BY Frequencies DESC
-- MAGIC """)
-- MAGIC
-- MAGIC # Convert the SQL query result to a pandas DataFrame
-- MAGIC
-- MAGIC df_2023 = sql_query_result.toPandas()
-- MAGIC
-- MAGIC # Plot the data
-- MAGIC
-- MAGIC plt.figure(figsize=(10, 6))
-- MAGIC plt.bar(df_2023['Type'], df_2023['Frequencies'], color='skyblue')
-- MAGIC plt.xlabel('Type')
-- MAGIC plt.ylabel('Frequencies')
-- MAGIC plt.title('Types of Studies in 2023')
-- MAGIC plt.xticks(rotation=90)
-- MAGIC plt.show()
-- MAGIC

-- COMMAND ----------

--- Types of Studies for clinicaltrial_2021

SELECT DISTINCT(Type), COUNT(Type) AS Frequencies 
FROM ct_db.clinicaltrial_2021
WHERE Type IS NOT NULL AND LENGTH(Type) > 0
GROUP BY Type
ORDER BY Frequencies DESC


-- COMMAND ----------

--- Types of Studies for clinicaltrial_2020

SELECT DISTINCT(Type), COUNT(Type) AS Frequencies 
FROM ct_db.clinicaltrial_2020
WHERE Type IS NOT NULL AND LENGTH(Type) > 0
GROUP BY Type
ORDER BY Frequencies DESC


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Q3: Top 5 Conditions with their Frequencies

-- COMMAND ----------

--- Top 5 Conditions for clinicaltrial_2023

SELECT condition AS Top5_Conditions2023, COUNT(*) AS Frequencies
FROM (
  SELECT EXPLODE(SPLIT(conditions, '\\|')) AS condition
  FROM ct_db.clinicaltrial_2023
  WHERE conditions IS NOT NULL
)
GROUP BY condition
ORDER BY Frequencies DESC
LIMIT 5


-- COMMAND ----------

-- Top 5 Conditions for clinicaltrial_2021

SELECT condition AS Top5_Conditions2021, COUNT(*) AS Frequencies
FROM (
  SELECT EXPLODE(SPLIT(conditions, ',')) AS condition
  FROM ct_db.clinicaltrial_2021
  WHERE conditions IS NOT NULL
)
GROUP BY condition
ORDER BY Frequencies DESC
LIMIT 5


-- COMMAND ----------

--- Top 5 Conditions for clinicaltrial_2020

SELECT condition AS Top5_Conditions2020, COUNT(*) AS Frequencies
FROM (
  SELECT EXPLODE(SPLIT(conditions, ',')) AS condition
  FROM ct_db.clinicaltrial_2020
  WHERE conditions IS NOT NULL
)
GROUP BY condition
ORDER BY Frequencies DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Q4: Ten Most Common Sponsors that are not Pharmaceutical Companies, along with the Number of Clinical Trials they have Sponsored

-- COMMAND ----------

--- Ten Most Common Sponsors that are not Pharmaceutical Companies for clinicaltrial_2023

WITH common_sponsors AS (
    SELECT Sponsor AS NonPharmaSponsor_2023
    FROM ct_db.clinicaltrial_2023
    WHERE Sponsor NOT IN (
        SELECT Parent_Company 
        FROM ct_db.pharma
    )
)
SELECT NonPharmaSponsor_2023,
       COUNT(*) AS NumberOfClinicalTrials
FROM common_sponsors
GROUP BY NonPharmaSponsor_2023
ORDER BY NumberOfClinicalTrials DESC
LIMIT 10;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC ### Visualization of Ten Most Common Sponsors that are not Pharmaceutical Companies for clinicaltrial_2023
-- MAGIC
-- MAGIC import pandas as pd
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC
-- MAGIC # Execute SQL query and Store the result in a pandas DataFrame
-- MAGIC
-- MAGIC sql_query_result = spark.sql("""
-- MAGIC WITH common_sponsors AS (
-- MAGIC     SELECT Sponsor AS NonPharmaSponsor_2023
-- MAGIC     FROM ct_db.clinicaltrial_2023
-- MAGIC     WHERE Sponsor NOT IN (
-- MAGIC         SELECT Parent_Company 
-- MAGIC         FROM ct_db.pharma
-- MAGIC     )
-- MAGIC )
-- MAGIC SELECT NonPharmaSponsor_2023,
-- MAGIC        COUNT(*) AS NumberOfClinicalTrials
-- MAGIC FROM common_sponsors
-- MAGIC GROUP BY NonPharmaSponsor_2023
-- MAGIC ORDER BY NumberOfClinicalTrials DESC
-- MAGIC LIMIT 10;
-- MAGIC """)
-- MAGIC
-- MAGIC # Convert the SQL query result to a pandas DataFrame
-- MAGIC
-- MAGIC df_2023 = sql_query_result.toPandas()
-- MAGIC
-- MAGIC # Plot the data
-- MAGIC
-- MAGIC plt.figure(figsize=(12, 8))
-- MAGIC plt.barh(df_2023['NonPharmaSponsor_2023'], df_2023['NumberOfClinicalTrials'], color='skyblue')
-- MAGIC plt.xlabel('NumberOfClinicalTrials')
-- MAGIC plt.ylabel('NonPharmaSponsor_2023')
-- MAGIC plt.title('Ten Non Pharma Sponsor 2023')
-- MAGIC plt.gca().invert_yaxis()
-- MAGIC plt.tight_layout()
-- MAGIC plt.show()
-- MAGIC

-- COMMAND ----------

--- Ten Most Common Sponsors that are not Pharmaceutical Companies for clinicaltrial_2021

WITH common_sponsors AS (
    SELECT Sponsor AS NonPharmaSponsor_2021
    FROM ct_db.clinicaltrial_2021
    WHERE Sponsor NOT IN (
        SELECT Parent_Company 
        FROM ct_db.pharma
    )
)
SELECT NonPharmaSponsor_2021,
       COUNT(*) AS NumberOfClinicalTrials
FROM common_sponsors
GROUP BY NonPharmaSponsor_2021
ORDER BY NumberOfClinicalTrials DESC
LIMIT 10;


-- COMMAND ----------

--- Ten Most Common Sponsors that are not Pharmaceutical Companies for clinicaltrial_2020

WITH common_sponsors AS (
    SELECT Sponsor AS NonPharmaSponsor_2020
    FROM ct_db.clinicaltrial_2020
    WHERE Sponsor NOT IN (
        SELECT Parent_Company 
        FROM ct_db.pharma
    )
)
SELECT NonPharmaSponsor_2020,
       COUNT(*) AS NumberOfClinicalTrials
FROM common_sponsors
GROUP BY NonPharmaSponsor_2020
ORDER BY NumberOfClinicalTrials DESC
LIMIT 10;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Q5: Plot Number of Completed Studies for Each Month and Carete a Table of all Values

-- COMMAND ----------

--- Table Creation for Number of Completed Studies for Each Month in 2023 for clinicaltrial_2023

SELECT
  CASE MONTH(TO_DATE(Completion, 'yyyy-MM-dd'))
    WHEN 1 THEN 'Jan'
    WHEN 2 THEN 'Feb'
    WHEN 3 THEN 'Mar'
    WHEN 4 THEN 'Apr'
    WHEN 5 THEN 'May'
    WHEN 6 THEN 'Jun'
    WHEN 7 THEN 'Jul'
    WHEN 8 THEN 'Aug'
    WHEN 9 THEN 'Sep'
    WHEN 10 THEN 'Oct'
    WHEN 11 THEN 'Nov'
    WHEN 12 THEN 'Dec'
  END AS Month,
  COUNT(*) AS Completed_Studies_2023
FROM
  (
    SELECT
      CASE
        WHEN LENGTH(Completion) = 7 THEN CONCAT(Completion, '-01')
        ELSE Completion
      END AS Completion
    FROM ct_db.clinicaltrial_2023
    WHERE Status = 'COMPLETED'
  ) AS filtered_dates
WHERE Completion LIKE '2023%'
GROUP BY MONTH(TO_DATE(Completion, 'yyyy-MM-dd'))
ORDER BY MONTH(TO_DATE(Completion, 'yyyy-MM-dd'));


-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Plot Number of Completed Studies for Each Month in 2023 for clinicaltrial_2023
-- MAGIC
-- MAGIC import pandas as pd
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC
-- MAGIC # Execute SQL query and Store the result in a pandas DataFrame
-- MAGIC
-- MAGIC sql_query_result = spark.sql("""
-- MAGIC SELECT
-- MAGIC   CASE MONTH(TO_DATE(Completion, 'yyyy-MM-dd'))
-- MAGIC     WHEN 1 THEN 'Jan'
-- MAGIC     WHEN 2 THEN 'Feb'
-- MAGIC     WHEN 3 THEN 'Mar'
-- MAGIC     WHEN 4 THEN 'Apr'
-- MAGIC     WHEN 5 THEN 'May'
-- MAGIC     WHEN 6 THEN 'Jun'
-- MAGIC     WHEN 7 THEN 'Jul'
-- MAGIC     WHEN 8 THEN 'Aug'
-- MAGIC     WHEN 9 THEN 'Sep'
-- MAGIC     WHEN 10 THEN 'Oct'
-- MAGIC     WHEN 11 THEN 'Nov'
-- MAGIC     WHEN 12 THEN 'Dec'
-- MAGIC   END AS Month,
-- MAGIC   COUNT(*) AS Completed_Studies_2023
-- MAGIC FROM
-- MAGIC   (
-- MAGIC     SELECT
-- MAGIC       CASE
-- MAGIC         WHEN LENGTH(Completion) = 7 THEN CONCAT(Completion, '-01')
-- MAGIC         ELSE Completion
-- MAGIC       END AS Completion
-- MAGIC     FROM ct_db.clinicaltrial_2023
-- MAGIC     WHERE Status = 'COMPLETED'
-- MAGIC   ) AS filtered_dates
-- MAGIC WHERE Completion LIKE '2023%'
-- MAGIC GROUP BY MONTH(TO_DATE(Completion, 'yyyy-MM-dd'))
-- MAGIC ORDER BY MONTH(TO_DATE(Completion, 'yyyy-MM-dd'))
-- MAGIC """)
-- MAGIC
-- MAGIC # Convert the SQL query result to a pandas DataFrame
-- MAGIC
-- MAGIC df_2023 = sql_query_result.toPandas()
-- MAGIC
-- MAGIC # Plot the data
-- MAGIC
-- MAGIC plt.figure(figsize=(10, 6))
-- MAGIC plt.bar(df_2023['Month'], df_2023['Completed_Studies_2023'], color='skyblue')
-- MAGIC plt.xlabel('Month')
-- MAGIC plt.ylabel('Number of Completed Studies')
-- MAGIC plt.title('Number of Completed Studies in 2023 by Month')
-- MAGIC plt.xticks(rotation=45)
-- MAGIC plt.show()
-- MAGIC

-- COMMAND ----------

--- Table Creation for Number of Completed Studies for Each Month in 2021 for clinicaltrial_2021

SELECT
  date_format(to_date(Completion, 'MMM yyyy'),'MMM') AS Month,
  COUNT(*) AS Completed_Studies_2021
FROM ct_db.clinicaltrial_2021
WHERE
  Completion LIKE '%2021' AND Status = 'Completed'
GROUP BY Month
ORDER BY
  CASE 
    WHEN Month = 'Jan' THEN 1
    WHEN Month = 'Feb' THEN 2
    WHEN Month = 'Mar' THEN 3
    WHEN Month = 'Apr' THEN 4
    WHEN Month = 'May' THEN 5
    WHEN Month = 'Jun' THEN 6
    WHEN Month = 'Jul' THEN 7
    WHEN Month = 'Aug' THEN 8
    WHEN Month = 'Sep' THEN 9
    WHEN Month = 'Oct' THEN 10
    WHEN Month = 'Nov' THEN 11
    WHEN Month = 'Dec' THEN 12
  END;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Plot Number of Completed Studies for Each Month in 2021 for clinicaltrial_2021
-- MAGIC
-- MAGIC import pandas as pd
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC
-- MAGIC # Execute SQL query and Store the result in a pandas DataFrame
-- MAGIC
-- MAGIC sql_query_result = spark.sql("""
-- MAGIC SELECT
-- MAGIC   date_format(to_date(Completion, 'MMM yyyy'),'MMM') AS Month,
-- MAGIC   COUNT(*) AS Completed_Studies_2021
-- MAGIC FROM ct_db.clinicaltrial_2021
-- MAGIC WHERE
-- MAGIC   Completion LIKE '%2021' AND Status = 'Completed'
-- MAGIC GROUP BY Month
-- MAGIC ORDER BY
-- MAGIC   CASE 
-- MAGIC     WHEN Month = 'Jan' THEN 1
-- MAGIC     WHEN Month = 'Feb' THEN 2
-- MAGIC     WHEN Month = 'Mar' THEN 3
-- MAGIC     WHEN Month = 'Apr' THEN 4
-- MAGIC     WHEN Month = 'May' THEN 5
-- MAGIC     WHEN Month = 'Jun' THEN 6
-- MAGIC     WHEN Month = 'Jul' THEN 7
-- MAGIC     WHEN Month = 'Aug' THEN 8
-- MAGIC     WHEN Month = 'Sep' THEN 9
-- MAGIC     WHEN Month = 'Oct' THEN 10
-- MAGIC     WHEN Month = 'Nov' THEN 11
-- MAGIC     WHEN Month = 'Dec' THEN 12
-- MAGIC     END;
-- MAGIC """)
-- MAGIC
-- MAGIC # Convert the SQL query result to a pandas DataFrame
-- MAGIC
-- MAGIC df_2021 = sql_query_result.toPandas()
-- MAGIC
-- MAGIC # Plot the data
-- MAGIC
-- MAGIC plt.figure(figsize=(10, 6))
-- MAGIC plt.bar(df_2021['Month'], df_2021['Completed_Studies_2021'], color='skyblue')
-- MAGIC plt.xlabel('Month')
-- MAGIC plt.ylabel('Number of Completed Studies')
-- MAGIC plt.title('Number of Completed Studies in 2021 by Month')
-- MAGIC plt.xticks(rotation=45)
-- MAGIC plt.show()
-- MAGIC

-- COMMAND ----------

--- Table Creation for Number of Completed Studies for Each Month in 2020 for clinicaltrial_2020

SELECT
  date_format(to_date(Completion, 'MMM yyyy'),'MMM') AS Month,
  COUNT(*) AS Completed_Studies_2020
FROM ct_db.clinicaltrial_2020
WHERE
  Completion LIKE '%2020' AND Status = 'Completed'
GROUP BY Month
ORDER BY
  CASE 
    WHEN Month = 'Jan' THEN 1
    WHEN Month = 'Feb' THEN 2
    WHEN Month = 'Mar' THEN 3
    WHEN Month = 'Apr' THEN 4
    WHEN Month = 'May' THEN 5
    WHEN Month = 'Jun' THEN 6
    WHEN Month = 'Jul' THEN 7
    WHEN Month = 'Aug' THEN 8
    WHEN Month = 'Sep' THEN 9
    WHEN Month = 'Oct' THEN 10
    WHEN Month = 'Nov' THEN 11
    WHEN Month = 'Dec' THEN 12
  END;
  

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Plot Number of Completed Studies for Each Month in 2020 for clinicaltrial_2020
-- MAGIC
-- MAGIC import pandas as pd
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC
-- MAGIC # Execute SQL query and Store the result in a pandas DataFrame
-- MAGIC
-- MAGIC sql_query_result = spark.sql("""
-- MAGIC SELECT
-- MAGIC   date_format(to_date(Completion, 'MMM yyyy'),'MMM') AS Month,
-- MAGIC   COUNT(*) AS Completed_Studies_2020
-- MAGIC FROM ct_db.clinicaltrial_2020
-- MAGIC WHERE
-- MAGIC   Completion LIKE '%2020' AND Status = 'Completed'
-- MAGIC GROUP BY Month
-- MAGIC ORDER BY
-- MAGIC   CASE 
-- MAGIC     WHEN Month = 'Jan' THEN 1
-- MAGIC     WHEN Month = 'Feb' THEN 2
-- MAGIC     WHEN Month = 'Mar' THEN 3
-- MAGIC     WHEN Month = 'Apr' THEN 4
-- MAGIC     WHEN Month = 'May' THEN 5
-- MAGIC     WHEN Month = 'Jun' THEN 6
-- MAGIC     WHEN Month = 'Jul' THEN 7
-- MAGIC     WHEN Month = 'Aug' THEN 8
-- MAGIC     WHEN Month = 'Sep' THEN 9
-- MAGIC     WHEN Month = 'Oct' THEN 10
-- MAGIC     WHEN Month = 'Nov' THEN 11
-- MAGIC     WHEN Month = 'Dec' THEN 12
-- MAGIC     END;
-- MAGIC """)
-- MAGIC
-- MAGIC # Convert the SQL query result to a pandas DataFrame
-- MAGIC
-- MAGIC df_2020 = sql_query_result.toPandas()
-- MAGIC
-- MAGIC # Plot the data
-- MAGIC
-- MAGIC plt.figure(figsize=(10, 6))
-- MAGIC plt.bar(df_2020['Month'], df_2020['Completed_Studies_2020'], color='skyblue')
-- MAGIC plt.xlabel('Month')
-- MAGIC plt.ylabel('Number of Completed Studies')
-- MAGIC plt.title('Number of Completed Studies in 2020 by Month')
-- MAGIC plt.xticks(rotation=45)
-- MAGIC plt.show()
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Additional Analysis 3: Top 5 Pharmaceutical Companies with the Highest Total Penalty Amounts Imposed on them

-- COMMAND ----------

--- Top 5 Pharmaceutical Companies with the Highest Total Penalty Amounts

SELECT Parent_Company, SUM(CAST(REPLACE(REPLACE(Penalty_Amount, '$', ''), ',', '') AS FLOAT)) AS Total_Penalty
FROM ct_db.pharma
GROUP BY Parent_Company
ORDER BY Total_Penalty DESC
LIMIT 5;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC ### Visualization of Top 5 Pharmaceutical Companies with the Highest Total Penalty Amounts for pharma
-- MAGIC
-- MAGIC import pandas as pd
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC
-- MAGIC # Execute SQL query and Store the result in a pandas DataFrame
-- MAGIC
-- MAGIC sql_query_result = spark.sql("""
-- MAGIC SELECT Parent_Company, SUM(CAST(REPLACE(REPLACE(Penalty_Amount, '$', ''), ',', '') AS FLOAT)) AS Total_Penalty
-- MAGIC FROM ct_db.pharma
-- MAGIC GROUP BY Parent_Company
-- MAGIC ORDER BY Total_Penalty DESC
-- MAGIC LIMIT 5;
-- MAGIC """)
-- MAGIC
-- MAGIC # Convert the SQL query result to a pandas DataFrame
-- MAGIC
-- MAGIC df = sql_query_result.toPandas()
-- MAGIC
-- MAGIC # Plot the data
-- MAGIC
-- MAGIC plt.figure(figsize=(12, 6))
-- MAGIC plt.barh(df['Parent_Company'], df['Total_Penalty'], color='skyblue')
-- MAGIC for index, value in enumerate(df['Total_Penalty']):
-- MAGIC     plt.text(value, index, '{:,.2f}'.format(value), ha='left', va='center', fontsize=10, color='black')
-- MAGIC plt.xlabel('Total_Penalty')
-- MAGIC plt.ylabel('Parent_Company')
-- MAGIC plt.title('Top 5 Pharmaceutical Companies')
-- MAGIC plt.gca().invert_yaxis()
-- MAGIC plt.tight_layout()
-- MAGIC plt.show()
