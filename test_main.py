# Assuming this is in test_main.py

import pytest
from pyspark.sql import SparkSession
from main import initiate_spark_session, read_dataset, describe


@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder.master("local[2]").appName("Test Session").getOrCreate()


@pytest.fixture(scope="module")
def sample_data_path():
    return "dataset/Development of Average Annual Wages_1.csv"


def test_initiate_spark_session():
    session = initiate_spark_session("Test App")
    assert session is not None
    assert session.sparkContext.appName == "Test App"


def test_read_dataset(spark_session, sample_data_path):
    dataset = read_dataset(spark_session, sample_data_path)
    assert dataset is not None
    assert dataset.count() > 0
    assert "Country" in dataset.columns


def test_describe(spark_session, sample_data_path):
    dataset = read_dataset(spark_session, sample_data_path)
    description = describe(dataset)
    assert "summary" in description
