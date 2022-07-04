# coding:utf8

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession  # SparkSQL中的入口对象是SparkSession对象
from pyspark.sql.types import StructType, StringType

import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"


if __name__ == "__main__":
    # 构建SparkSession对象，这个对象是 构建器模式 通过builder方法来构建
    spark = SparkSession.builder\
        .appName('local[*]')\
        .config("spark.sql.shuffle.partitions", "4")\
        .getOrCreate()
    # appName 设置程序名称，config设置一些常用属性
    # 最后通过 getCreate() 方法 创建SparkSession对象

    sc = spark.sparkContext   # 可以通过SparkSession对象的sparkContext属性获取 可用于SparkCore的sc对象

    # from text file
    schema = StructType().add("data", StringType(), nullable=True)
    df_txt = spark.read.format("text").\
        schema(schema=schema).\
        load("../stu_score.txt")
    # df_txt.printSchema()
    # df_txt.show()

    # from json file
    df_json = spark.read.format("json").\
        load("../stu_score.json")
    # df_json.printSchema()
    # df_json.show()

    # from csv file
    df_csv = spark.read.format("csv").\
        option("sep", ",").\
        option("header", True).\
        option("encoding", "utf-8").\
        schema("id INTEGER, subject STRING, score INTEGER").\
        load("../stu_score.csv")
    df_csv.printSchema()
    df_csv.show()
