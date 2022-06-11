# coding:utf8

from pyspark import SparkContext, SparkConf
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 7), ('b', 9), ('a', 5), ('c', 4)])
    print(rdd.collect())

    rdd2 = sc.parallelize([('a', 1), ('b', 1), ('b', 3), ('b', 4), ('d', 2)])
    print(rdd2.collect())

    rdd3 = rdd.join(rdd2)
    print(rdd3.collect())

    rdd4 = rdd.leftOuterJoin(rdd2)
    print(rdd4.collect())

    rdd5 = rdd.rightOuterJoin(rdd2)
    print(rdd5.collect())
