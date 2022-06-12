# coding:utf8

from pyspark import SparkContext, SparkConf
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"


def process(iter):
    return list(map(lambda x: x * 10, iter))


if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd2 = sc.parallelize(range(1, 10), 3)
    print(rdd2.getNumPartitions())
    rdd3 = rdd2.mapPartitions(process)
    print(rdd3.collect())