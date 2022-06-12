# coding:utf8

from pyspark import SparkContext, SparkConf
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"


def process(iter):
    for each in iter:
        print("* {} *".format(each))


if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd2 = sc.parallelize(range(1, 10), 3)
    print(rdd2.getNumPartitions())
    rdd2.foreachPartition(process)
