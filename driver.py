import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def main():
    spark = SparkSession.builder.appName("WeatherReader").getOrCreate()

    df_raw_one = spark.read.csv(
        "input_files/data_one_header.csv", header=True, inferSchema=True
    )

    first = df_raw_one.first()
    dfone = df_raw_one.filter(
        ~(df_raw_one[df_raw_one.columns[0]] == first[df_raw_one.columns[0]])
    )
    dfone.show()
    dfone.printSchema()

    df_raw_two = spark.read.csv(
        "input_files/data_two_header.csv", header=True, inferSchema=True
    )

    firsttwo = df_raw_two.first()
    dftwo = df_raw_two.filter(
        ~(df_raw_two[df_raw_two.columns[0]] == firsttwo[df_raw_two.columns[0]])
    )
    dftwo.show()
    dftwo.printSchema()

    df_full = dfone.join(dftwo, ["Station", "Date"], "inner")
    df_full.show()

    # datacolumns = ["Avg Temp","RH Max","Vapor Pressure","Liquid Precip","Gust Speed", "Gust Dir", "Solar Rad"]
    # condition = " & ".join([f"(col('{c}') != -999)" for c in datacolumns])
    df_full = (
        df_full.withColumn("RH Max", col("RH Max").cast("double"))
        .withColumn("Vapor Pressure", col("Vapor Pressure").cast("double"))
        .withColumn("Liquid Precip", col("Liquid Precip").cast("double"))
        .withColumn("Gust Speed", col("Gust Speed").cast("double"))
        .withColumn("Gust Dir", col("Gust Dir").cast("double"))
        .withColumn("Solar Rad", col("Solar Rad").cast("double"))
        .withColumn("Avg Temp", col("Avg Temp").cast("double"))
    )

    df_fixed = df_full.filter(
        (col("RH Max") != -999)
        & (col("Vapor Pressure") != -999)
        & (col("Liquid Precip") != -999)
        & (col("Gust Speed") != -999)
        & (col("Gust Dir") != -999)
        & (col("Solar Rad") != -999)
        & (col("Avg Temp") != -999)
    )
    df_fixed.show()

    cutoff = df_fixed.agg(
        functions.percentile_approx(col("Liquid Precip"), 0.95).alias("cutoff")
    ).collect()[0]["cutoff"]
    print(cutoff)
    # df_highprecip = df_fixed.filter(col("Liquid Precip") >= cutoff)
    df_fixed = df_fixed.withColumn(
        "High_Precip", functions.when(col("Liquid Precip") >= cutoff, 1).otherwise(0)
    )
    df_fixed.show()

    feature_cols = [
        "Avg Temp",
        "RH Max",
        "Vapor Pressure",
        "Gust Speed",
        "Gust Dir",
        "Solar Rad",
    ]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_ml = assembler.transform(df_fixed)

    train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)

    rf = RandomForestClassifier(labelCol="High_Precip", featuresCol="features", numTrees=100)
    model = rf.fit(train_df)

    predictions = model.transform(test_df)
    predictions.select("High_Precip", "prediction", "probability").show(10)

    evaluator = MulticlassClassificationEvaluator(
        labelCol="High_Precip", predictionCol="prediction", metricName="accuracy"
    )

    accuracy = evaluator.evaluate(predictions)
    print(f"Accuracy = {accuracy}")


if __name__ == "__main__":
    main()
