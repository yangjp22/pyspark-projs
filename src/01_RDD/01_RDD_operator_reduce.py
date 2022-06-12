# coding:utf8

from pyspark import SparkContext, SparkConf
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])
    print(rdd.reduce(lambda a, b: a + b))

    rdd2 = sc.parallelize([('a', 1), ('a', 2), ('b', 1), ('b', 1), ('a', 2)])
    print(rdd2.reduce(lambda a, b: (a[0], a[1] + b[1])))
