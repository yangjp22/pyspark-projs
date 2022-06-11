# coding:utf8

from pyspark import SparkContext, SparkConf
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 2, 1, 3, 4, 2])
    print(rdd.distinct().collect())

    rdd2 = sc.parallelize([('a', 1), ('a', 2), ('b', 1), ('b', 1), ('a', 2)])
    print(rdd2.distinct().collect())
