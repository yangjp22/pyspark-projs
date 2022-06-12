# coding:utf8

from pyspark import SparkContext, SparkConf
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd2 = sc.parallelize([('a', 1), ('d', 2), ('c', 1),
                           ('b', 1), ('s', 2), ('U', 3),
                           ('w', 2), ('f', 9), ('A', 2),
                           ('J', 2), ('D', 2), ('F', 7)])
    print(rdd2.collect())
    print(rdd2.sortByKey(numPartitions=1).collect())
    print(rdd2.sortByKey(numPartitions=1, keyfunc=lambda x: x.lower()).collect())
