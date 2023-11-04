from pyspark.sql import SparkSession
from pyspark.sql.functions import col, median
from pyspark.sql.types import (
    StructType,
    StructField,
    FloatType,
    StringType)

REPORT_FILE = "analysis_report.md"

def append_to_report(description, content, sql_query=None):
    with open(REPORT_FILE, "a") as report:
        report.write(f"## {description}\n\n")
        if sql_query:
            report.write(f"**SQL Query:**\n```sql\n{sql_query}\n```\n\n")
        report.write("**Result Preview:**\n\n")
        report.write(f"```markdown\n{content}\n```\n\n")

def initiate_spark_session(app_title):
    session = SparkSession.builder.appName(app_title).getOrCreate()
    return session

def read_dataset(spark, dataset_path):
    # Update the schema to match the structure of the new CSV file
    country_schema = StructType([
        StructField("Country", StringType(), True),
        StructField("year_2000", FloatType(), True),
        StructField("year_2010", FloatType(), True),
        StructField("year_2020", FloatType(), True),
        StructField("year_2022", FloatType(), True)
    ])
    
    # Reading the CSV file considering it may not have a header and specifying the separator
    dataset = spark.read.schema(country_schema).option("header", "true").csv(dataset_path)
    
    # Update the append_to_report call to reflect the loaded data
    append_to_report("Data Loading", dataset.limit(10).toPandas().to_markdown()) 
    return dataset

def describe(dataset):
    # The describe function doesn't need changes unless we want to handle missing values specifically
    description = dataset.describe().toPandas().to_markdown()
    append_to_report("Data Description", description)
    return description

# Additional function to handle the missing values (optional, depending on requirements)
def handle_missing_values(dataset):
    # Compute the median for each column that needs missing values filled.
    # This process involves creating a list of columns for which you want to calculate the median.
    columns_to_impute = ['year_2000', 'year_2010', 'year_2020', 'year_2022']
    # Calculate the median for each column and store it in a dictionary
    medians = {column: dataset.stat.approxQuantile(column, [0.5], 0.001)[0] for column in columns_to_impute}
    # Fill missing values with the corresponding median value
    for column, median_value in medians.items():
        dataset = dataset.na.fill({column: median_value})
    # After imputation, add an entry to the report
    append_to_report("Missing Values Handled", dataset.limit(10).toPandas().to_markdown())
    return dataset
