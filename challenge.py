import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
COL_DICT = {
    "id": "client_identifier",
    "btc_a": "bitcoin_address",
    "cc_t": "credit_card_type"
}


def filter_by_country(df, cont_1, cont_2):
    """Filtering DataFrame according to two countries given as param.

    "OR" operation has been used.Function returns both countries results.

    :param dataframe object df: PySpark Dataframe object.
    :param str cont_1: First country for filtering.
    :param str cont_2: Second country for filtering.
    :return: Dataframe that includes result of both countries.
    :rtype: PySpark Dataframe Object


    """
    return df.filter((df.country == cont_1) | (df.country == cont_2))


def read_csv(df_name, df2_name):
    """Reading csv files which is given as param(str) and converting them into PySpark Dataframes .

    :param str df_name: First Csv file name.
    :param str df2_name: Second Csv file name.
    :return: Two Pyspark Dataframes.
    :rtype: PySpark Dataframe Objects


    """

    df = spark.read.csv(f"{df_name}", header=True)
    df_2 = spark.read.csv(f"{df2_name}", header=True)

    return df, df_2


def col_rename(col_dict, df_con_rename):
    """Chancing name of the colums of Dataframe according to dict given as param.

    :param dict col_dict: Dictionary which includes old and new names of columns.
    :param DataFrame df_con_rename: Pyspark Dataframe object for column renaming.
    :return: Dataframe that includes with new column renames.
    :rtype: PySpark Dataframe Object


    """

    for ex, nex in col_dict.items():
        df_con_rename = df_con_rename.withColumnRenamed(ex, nex)
    return df_con_rename


def main():
    cont_1 = sys.argv[1]
    cont_2 = sys.argv[2]
    df_name = sys.argv[3]
    df2_name = sys.argv[4]
    log4jLogger = sc._jvm.org.apache.log4j
    log4jLogger.LogManager.getLogger(__name__)
    df, df_2 = read_csv(df_name, df2_name)
    df_fil = filter_by_country(df, cont_1, cont_2)
    df_fil = df_fil.drop("first_name", "last_name", "country")
    df_2 = df_2.drop("cc_n")
    df_con = df_fil.join(df_2, ['id'])
    df_con_rename = col_rename(COL_DICT, df_con)
    df_con_rename.toPandas().to_csv('./client_data/mycsv.csv')


if __name__ == '__main__':
    main()
