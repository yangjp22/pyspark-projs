# coding:utf8

from pyspark import SparkContext, SparkConf
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd2 = sc.parallelize([('a', 1), ('a', 2), ('b', 1), ('b', 4), ('a', 3)])
    print(rdd2.collect())
    print(rdd2.takeOrdered(2, lambda x: -x[1]))
