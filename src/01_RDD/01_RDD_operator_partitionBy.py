# coding:utf8

from pyspark import SparkContext, SparkConf
import os, functools

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"


def partitionF(x):
    if x % 3 == 0:
        return 0
    return 1 if x % 3 == 1 else 2


if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)
    print(rdd.glom().collect())
    rdd = rdd.map(lambda x: (x, x))
    rdd2 = rdd.partitionBy(3, partitionF)
    print(rdd2.glom().collect())

