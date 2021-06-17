import sys
import argparse
from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
COL_DICT = {
    "id": "client_identifier",
    "btc_a": "bitcoin_address",
    "cc_t": "credit_card_type"
}


def filter_by_country(df, cont_list):
    """Filtering DataFrame according to countries given as param.


    :param dataframe object df: PySpark Dataframe object.
    :param str cont_list: list countries for filtering.
    :return: Dataframe that includes result of country.
    :rtype: PySpark Dataframe Object


    """
    return df.filter((df.country.isin(cont_list)))


def read_csv(df_name, df2_name):
    """Reading csv files which is given as param(str) and converting them into PySpark Dataframes .

    :param str df_name: First Csv file name.
    :param str df2_name: Second Csv file name.
    :return: Two Pyspark Dataframes.
    :rtype: PySpark Dataframe Objects


    """

    df = spark.read.csv(f"./data_sets/{df_name}", header=True)
    df_2 = spark.read.csv(f"./data_sets/{df2_name}", header=True)

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
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--list', help='list of countries for filtering', type=str)
    parser.add_argument("-d", help="csv_1 to read", type=str)
    parser.add_argument("-d2", help="csv_2 to read", type=str)
    args = parser.parse_args()
    country_list = [str(item) for item in args.list.split(',')]
    df_name = args.d
    df2_name = args.d2
    log4jLogger = sc._jvm.org.apache.log4j
    log4jLogger.LogManager.getLogger(__name__)
    df, df_2 = read_csv(df_name, df2_name)
    df_fil = filter_by_country(df, country_list)
    df_fil = df_fil.drop("first_name", "last_name")
    df_2 = df_2.drop("cc_n")
    df_con = df_fil.join(df_2, ['id'])
    df_con_rename = col_rename(COL_DICT, df_con)
    df_con_rename.toPandas().to_csv('C:/Users/zafer/Desktop/abn_amro/proje_abn_amro/client_data/mycsv.csv')


if __name__ == '__main__':
    main()
