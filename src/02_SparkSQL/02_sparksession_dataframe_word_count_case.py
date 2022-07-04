# coding:utf8

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StringType
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"


if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName('local[*]')\
        .config("spark.sql.shuffle.partitions", "4")\
        .getOrCreate()

    # SQL style
    sc = spark.sparkContext
    rdd = sc.textFile("../word.txt").\
        flatMap(lambda x: x.split(" ")).\
        map(lambda x: x.strip(".")).\
        map(lambda x: x.strip(",")).\
        map(lambda x: [x])

    df = rdd.toDF(['word'])
    # df.show()

    df.createTempView('words')
    # spark.sql('select word, count(*) as cnt from words group by word order by cnt desc').show()


    # DSL 风格
    df_dsl = spark.read.format('text').\
        schema(StructType().add("word", StringType(), nullable=True)).\
        load('../word.txt')
    df_dsl_02 = df_dsl.withColumn("new_word", F.explode(F.split(df_dsl['word'], " ")))
    df_dsl_02.groupby('new_word').count().show()
