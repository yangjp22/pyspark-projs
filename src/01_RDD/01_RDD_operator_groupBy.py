# coding:utf8

from pyspark import SparkContext, SparkConf
import os, functools

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"


def twoGroups(x):
    return "odd" if x % 2 != 0 else "even"


def threeGroups(x):
    if x % 3 == 0:
        return "Exactly"
    return "1_more" if x % 3 == 1 else "2_more"


if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])
    print(rdd.getNumPartitions())
    print(rdd.collect())

    rdd_2 = rdd.groupBy(twoGroups)
    rdd_2_v = rdd_2.map(lambda x: (x[0], list(x[1])))
    print(rdd_2_v.collect())

    rdd_3 = rdd.groupBy(threeGroups).map(lambda x: (x[0], list(x[1])))
    print(rdd_3.collect())

    rdd_4 = rdd_3.mapValues(lambda x: functools.reduce(lambda a, b: a + b, x))
    print(rdd_4.collect())
