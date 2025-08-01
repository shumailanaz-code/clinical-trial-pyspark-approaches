🧪 Clinical Trial Data Analysis using PySpark (RDD, DataFrame, SQL)

👩‍💻 Overview
This project analyzes a real-world clinical trial dataset using three different PySpark paradigms:

RDD-based implementation

DataFrame API

Spark SQL

As a data scientist tasked by a healthcare client, the goal is to derive insights from clinical trial records and support those insights with visualizations and aggregated statistics. Each method showcases a different way of working with big data using PySpark on Databricks.

🎯 Objectives
The project addresses the following key questions:

Total number of distinct studies

Frequency of each study type (sorted from most to least frequent)

Top 5 most common conditions in the dataset

Top 10 non-pharmaceutical sponsors and their clinical trial counts

Monthly count of completed studies in 2023, including:

📊 A bar graph

📋 A tabular summary

Additionally, further exploratory analyses and data visualizations were included in each implementation to provide deeper insights.

🛠 Technologies Used
Apache Spark (PySpark) in Databricks

Spark RDD, DataFrame, SQL APIs

Matplotlib / Seaborn / Pandas (for graphing)

Jupyter Notebooks / Databricks Notebooks

Python

📁 Project Structure
clinical-trial-pyspark-approaches/
├── pyspark_dataframe.py       # Analysis using Spark DataFrame API
├── pyspark_rdd.py             # Analysis using RDD transformations
├── pyspark_sql.py             # Analysis using SQL queries
└── README.md

📊 Visualizations
Each approach includes visual charts to enhance understanding:

Bar charts for study types

Frequency plots for conditions

Monthly line/bar charts of completed trials (2023)

Sponsor frequency comparisons

📌 Data Preprocessing
Before performing analysis, the following preprocessing steps were applied:

Removed duplicates

Handled missing values

Extracted and formatted date fields

Normalized text entries (e.g., company names)

Filtered valid records for time-based analysis

📚 Data Source
The dataset is sourced from clinicaltrials.gov and https://violationtracker.goodjobsfirst.org/industry/pharmaceuticals.

