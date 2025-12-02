import pyspark

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("WeatherReader") \
        .getOrCreate()

    df_raw_one = spark.read.csv("input_files/data_one_header.csv", header=True, inferSchema=True)

    first = df_raw_one.first()
    dfone = df_raw_one.filter(~(df_raw_one[df_raw_one.columns[0]] == first[df_raw_one.columns[0]]))
    dfone.show()
    dfone.printSchema()

    df_raw_two = spark.read.csv("input_files/data_two_header.csv", header=True, inferSchema=True)

    firsttwo = df_raw_two.first()
    dftwo = df_raw_two.filter(~(df_raw_two[df_raw_two.columns[0]] == firsttwo[df_raw_two.columns[0]]))
    dftwo.show()
    dftwo.printSchema()


if __name__ == '__main__':
    main()
