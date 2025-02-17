import pytest
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from utils.utilities import (
    load_file_into_pandas_df,
    merge_dataframes,
    df_as_string,
    transform_data,
    apply_filters,
    aggregate_data,
)


# Setup Spark session for tests
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


# Sample DataFrames
@pytest.fixture
def sample_pandas_df():
    data = {"DeviceID": [1, 2, 3], "Value": [10, 20, 30]}
    return pd.DataFrame(data)


@pytest.fixture
def sample_spark_df(spark):
    data = [(1, 10), (2, 20), (3, 30)]
    return spark.createDataFrame(data, ["DeviceID", "Value"])


# Test load_file_into_pandas_df
def test_load_file_into_pandas_df(mocker, tmp_path):
    mock_logger = mocker.patch("app.utils.logger.get_logger")
    test_file = tmp_path / "test.csv"
    test_file.write_text("DeviceID,Value\n1,10\n2,20")

    df = load_file_into_pandas_df(str(test_file), "test_job")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    mock_logger.assert_called()


# Test merge_dataframes
def test_merge_dataframes(mocker, tmp_path):
    mock_logger = mocker.patch("app.utils.logger.get_logger")
    file1 = tmp_path / "file1.csv"
    file2 = tmp_path / "file2.csv"
    file1.write_text("DeviceID,Value\n1,10\n")
    file2.write_text("DeviceID,Value\n2,20\n")

    df = merge_dataframes([str(file1), str(file2)], "test_job")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    mock_logger.assert_called()


# Test df_as_string
def test_df_as_string(sample_spark_df):
    result = df_as_string(sample_spark_df, n=2, truncate=True)
    assert isinstance(result, str)


# Test transform_data
def test_transform_data(spark, mocker):
    mock_logger = mocker.patch("app.utils.logger.get_logger")
    data = [(1, "2023-01-01"), (2, "2023-02-01")]
    df = spark.createDataFrame(data, ["DeviceID", "Timestamp"])
    df.write.format("delta").mode("overwrite").save("/tmp/test_delta")

    conf = {
        "columns": {
            "unchanged": ["DeviceID"],
            "transformed": [{"expression": "CAST(Timestamp AS DATE)", "name": "Date"}],
        }
    }

    transformed_df = transform_data(spark, "test_job", "/tmp/test_delta", conf)
    assert "Date" in transformed_df.columns
    mock_logger.assert_called()


# Test apply_filters
def test_apply_filters(spark):
    data = [(1, "Sensor", "irradiance"), (2, "Satellite", "temperature")]
    df = spark.createDataFrame(data, ["DeviceID", "DeviceType", "Metric"])

    filters = [
        {"column": "DeviceType", "condition": "IN", "value": ["Sensor", "Satellite"]},
        {"column": "Metric", "condition": "==", "value": "irradiance"},
    ]

    filtered_df = apply_filters(df, filters)
    assert filtered_df.count() == 1


# Test aggregate_data
def test_aggregate_data(spark):
    data = [(1, "2023-01-01 10:00", 10), (1, "2023-01-01 10:00", 20)]
    df = spark.createDataFrame(data, ["DeviceID", "Hour_Timestamp", "Value"])

    conf = {
        "irradiance" : {
        "aggregation": {
            "group_by": ["DeviceID", "Hour_Timestamp"],
            "metrics": [{"name": "AvgValue", "function": "avg", "column": "Value"}],
        }
    }
    }

    aggregated_df = aggregate_data("aggregation", df, conf)
    assert aggregated_df.count() == 1
