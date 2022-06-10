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
    print(rdd.getNumPartitions())

    rdd2 = rdd.reduceByKey(lambda a, b: a * b)
    print(rdd2.collect())
    print(rdd2.getNumPartitions())
