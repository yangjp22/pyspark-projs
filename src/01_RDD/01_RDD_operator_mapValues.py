# coding:utf8

from pyspark import SparkContext, SparkConf
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 3), ('a', 2), ('a', 1), ('b', 2), ('c', 2)])
    print(rdd.collect())

    rdd1 = rdd.map(lambda x: (x[0], x[1] * 10))
    print(rdd1.collect())

    rdd2 = rdd.mapValues(lambda x: x * 10)
    print(rdd2.collect())