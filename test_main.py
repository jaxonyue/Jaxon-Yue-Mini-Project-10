import pytest
from mylib.lib import initiate_spark_session, read_dataset, describe

@pytest.fixture(scope="session")
def spark_session():
    return initiate_spark_session("Test Country Wages Analysis")

# The fixture "spark_session" is automatically used in tests that list it as an argument
def test_data_loading(spark_session):
    data_path = "data/Development of Average Annual Wages_1.csv"
    df = read_dataset(spark_session, data_path)
    assert df is not None
    assert df.count() > 0

def test_data_describe(spark_session):
    df = read_dataset(spark_session, "data/Development of Average Annual Wages_1.csv")
    description_data = describe(df)
    assert description_data is not None