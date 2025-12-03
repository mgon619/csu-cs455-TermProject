import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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

    df_full = dfone.join(dftwo, ["Station", "Date"], "inner")
    df_full.show()

    #datacolumns = ["RH Max","Vapor Pressure","Liquid Precip","Gust Speed", "Gust Dir", "Solar Rad"]
    #condition = " & ".join([f"(col('{c}') != -999)" for c in datacolumns])
    df_fixed = df_full.filter(
        (col("RH Max").cast("double") != -999) & 
        (col("Vapor Pressure").cast("double") != -999) &
        (col("Liquid Precip").cast("double") != -999) & 
        (col("Gust Speed").cast("double") != -999) & 
        (col("Gust Dir").cast("double") != -999) & 
        (col("Solar Rad").cast("double") != -999) 
    )
    df_fixed.show()

if __name__ == '__main__':
    main()
