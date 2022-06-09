# coding:utf8
from pyspark import SparkConf, SparkContext
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"


if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    rdd = sc.wholeTextFiles("../tiny_files")
    cnt_rdd = rdd.map(lambda x: x[1])
    print(cnt_rdd.collect())