import pytest
from pyspark.sql.session import SparkSession
from mylib.lib import initiate_spark_session, read_dataset
from mylib.lib import describe, handle_missing_values


@pytest.fixture(scope="session")
def spark_session():
    session = initiate_spark_session("Test Country Wages Analysis")
    yield session
    session.stop()


def test_data_loading(spark_session: SparkSession):
    data_file_path = "data/Development of Average Annual Wages_1.csv"
    df = read_dataset(spark_session, data_file_path)
    assert df is not None
    assert df.count() > 0


def test_data_describe(spark_session: SparkSession):
    data_file_path = "data/Development of Average Annual Wages_1.csv"
    df = read_dataset(spark_session, data_file_path)
    description_data = describe(df)
    assert description_data is not None


def test_handle_missing_values(spark_session: SparkSession):
    data_file_path = "data/Development of Average Annual Wages_1.csv"
    df = read_dataset(spark_session, data_file_path)
    description_data = handle_missing_values(df)
    assert description_data is not None
