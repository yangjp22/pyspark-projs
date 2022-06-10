# coding:utf8

from pyspark import SparkContext, SparkConf
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(["a b c", "d e f", "g h i"], 3)
    print(rdd.collect())

    rdd2 = rdd.flatMap(lambda x: x.split(" "))
    print(rdd2.collect())

    rdd3 = rdd.map(lambda x: x.split(" "))
    print(rdd3.collect())