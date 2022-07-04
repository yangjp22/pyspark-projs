# coding:utf8

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession  # SparkSQL中的入口对象是SparkSession对象
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

    df = spark.read.csv("../stu_score.txt", sep=',', header=False)
    df2 = df.toDF('id', 'name', 'score')
    # df2.printSchema()
    # df2.show()

    df2.createTempView('score')   # 创建一个临时视图, 视图名称是叫score

    #  SQL 风格
    spark.sql('''
        SELECT * FROM score WHERE name='语文'
    ''').show()

    # DSL 风格
    df2.where("name='语文'").show()
