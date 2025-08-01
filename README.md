# 🧪 Clinical Trial Data Analysis using PySpark (RDD, DataFrame, SQL)

## 👩‍💻 Overview

This project analyzes a real-world clinical trial dataset using three different PySpark paradigms:

- 🔹 **RDD-based implementation**
- 🔹 **DataFrame API**
- 🔹 **Spark SQL**

The objective is to extract meaningful insights from clinical trial data by addressing specific analytical questions and 
supporting them with visualizations. Each implementation includes additional exploratory analysis beyond the core requirements.

---

## 📌 Key Questions Answered

1. **📊 Total Number of Distinct Studies**  
   Ensured that only unique studies were counted to avoid duplication.

2. **📁 Study Types & Frequencies**  
   Listed all unique study types (from the `Type` column), ordered from most to least frequent.

3. **🩺 Top 5 Medical Conditions**  
   Extracted and ranked the five most frequently occurring conditions.

4. **🏥 Top 10 Non-Pharmaceutical Sponsors**  
   Identified sponsors excluding pharmaceutical companies (as flagged in the `Parent Company` column), along with the number of trials each conducted.

5. **📅 Completed Studies in 2023 (Monthly)**  
   Visualized the number of completed studies per month in 2023, alongside a data table with exact counts.

---

## 🛠 Additional Features

- ✅ **Preprocessing**: Cleaned raw clinical data before analysis.
- 📈 **Visualizations**: Graphs generated for key questions to aid interpretation.
- 🔍 **Comparative Approach**: Each paradigm (RDD, DataFrame, SQL) presents an independent solution with added insights.

---

## 🛠 Technologies Used

- **Apache Spark (PySpark)** in **Databricks**
- **Spark RDD**, **DataFrame**, and **SQL APIs**
- **Matplotlib**, **Seaborn**, **Pandas** (for graphing)
- **Jupyter Notebooks** / **Databricks Notebooks**
- **Python**

---

## 📁 Project Structure

```clinical-trial-pyspark-approaches/```
```1. PySpark Dataframe Implementation.py # Analysis using PySpark DataFrames```
```2. PySpark RDD Implementation.py # Analysis using PySpark RDDs```
```3. PySpark SQL Implementation.py # Analysis using Spark SQL```
```4. README.md```
