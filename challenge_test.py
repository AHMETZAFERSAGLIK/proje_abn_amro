from pyspark.sql import SparkSession
from challenge import read_csv, filter_by_country, COL_DICT, col_rename
from chispa.dataframe_comparer import *
spark = SparkSession.builder.getOrCreate()

df_name = "dataset_one.csv"
df2_name = "dataset_two.csv"


def test_read():
    """Test for Reading csv.

    Checks the function of read_csv by comparing row size.

    """

    df, df_2 = read_csv(df_name, df2_name)
    assert df.count() == 1000
    assert df_2.count() == 1000


def test_filter():
    """Test for Filtering dataframe.

    Checks the function of filter_by_country by comparing with test Dataframe.

    """

    data = [("Netherlands",), ("United Kingdom",), ("France",)]
    input_df = spark.createDataFrame(data, ["country"])
    exp_data = [("Netherlands",), ("United Kingdom",)]
    exp_input_df = spark.createDataFrame(exp_data, ["country"])
    result_df = filter_by_country(input_df, "United Kingdom", "Netherlands")
    assert_df_equality(result_df, exp_input_df)


