# coding:utf8

from pyspark import SparkContext, SparkConf
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 1)
    print(rdd.fold(10, lambda x, y: x + y))
    
    rdd2 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)
    print(rdd2.fold(10, lambda x, y: x + y))
