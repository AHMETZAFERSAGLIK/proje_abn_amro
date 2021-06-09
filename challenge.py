import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()



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




def main():
    cont_1 = sys.argv[1]
    cont_2 = sys.argv[2]
    df_name = sys.argv[3]
    df2_name = sys.argv[4]
    log4jLogger = sc._jvm.org.apache.log4j
    log4jLogger.LogManager.getLogger(__name__)



if __name__ == '__main__':
    main()
