# IDS 706 Mini Project 10 [![CI](https://github.com/jaxonyue/Jaxon-Yue-Mini-Project-10/actions/workflows/cicd.yml/badge.svg)](https://github.com/jaxonyue/Jaxon-Yue-Mini-Project-10/actions/workflows/cicd.yml)

## Overview
* This repository includes the components for Mini-Project 10 - PySpark Data Processing

## Goal
* Use PySpark to perform data processing on a large dataset
* Include at least one Spark SQL query and one data transformation

## Key Elements in the Repo:
* dataset/Development of Average Annual Wages_1.csv (the dataset used in this project)
* mylib/lib.py (contains the PySpark script)
* analysis_report.md (summary report)
* main.py
* test_main.py
* Makefile
* Dockerfile
* devcontainer
* GitHub Actions

## PySpark Script
I have implemented the following PySpark functions to perform data processing on the average wages dataset:
* `append_to_report`: appends content to the analysis report
* `initiate_spark_session`: creates and returns a new Spark session
* `read_dataset`: reads a CSV file with a predefined schema
* `describe`: generates descriptive statistics of a dataframe (count, mean, standard deviation, min and max)
* `handle_missing_values`: as the data transformation function, imputes the missing values in the dataset with the median of each column

## Summary Report (analysis_report.md)
Created by the script, the report contains the previews of the dataset after initial loading and handling missing values, as well as the descriptive statistics of the dataset and the result after the SQL query shown below.
* Returns the top five countries with the highest average wages from 2000 to 2022:
```
SELECT Country, AVG(((year_2000 + year_2010 + year_2020 + year_2022) / 4))
AS AvgWage
FROM country_data_view
GROUP BY Country
ORDER BY AvgWage DESC
LIMIT 5
```

I have passed all GitHub Actions as below:
<img width="915" alt="Screenshot 2023-11-04 at 10 32 14 PM" src="https://github.com/jaxonyue/Jaxon-Yue-Mini-Project-10/assets/70416390/39d63c95-c4a1-48e7-b8f9-2264bc65c144">
