# coding:utf8

from pyspark import SparkContext, SparkConf
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 3, 6, 7, 8, 9])
    print(rdd.collect())

    rdd2 = sc.parallelize([2, 3, 5, 6, 8, 2, 3, 10, 12])
    print(rdd2.collect())

    print(rdd.intersection(rdd2).collect())
