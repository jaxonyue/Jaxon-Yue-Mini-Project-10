from mylib.lib import (
    initiate_spark_session,
    read_dataset,
    describe,
    append_to_report,
    handle_missing_values,
)


def run_data_analysis():
    spark = initiate_spark_session("Country Wages Analysis")

    # Update data file path to the correct location
    data_file_path = "data/Development of Average Annual Wages_1.csv"
    country_data = read_dataset(spark, data_file_path)
    country_data.createOrReplaceTempView("country_data_view")
    country_data = handle_missing_values(country_data)
    describe(country_data)

    # Select the average wage by country for the years in the dataset:
    query_result = spark.sql(
        """
        SELECT Country, AVG(((year_2000 + year_2010 + year_2020 + year_2022) / 4))
        AS AvgWage
        FROM country_data_view
        GROUP BY Country
        ORDER BY AvgWage DESC
        LIMIT 5
        """
    )
    query_result.show()
    append_to_report(
        "Spark SQL Query Result", query_result.limit(10).toPandas().to_markdown()
    )
    spark.stop()


if __name__ == "__main__":
    run_data_analysis()
