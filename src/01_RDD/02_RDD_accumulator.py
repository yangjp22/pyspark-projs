# coding:utf8

from pyspark import SparkContext, SparkConf
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])

    """
    >>  没有使用累加器时的情况
        sums = 0
        def mapF(data):
            global sums
            sums += data
            print(sums)
        rdd.map(mapF).collect()
        print(sums)
    """
    sums = sc.accumulator(0)
    def func(data):
        global sums
        sums += data
    rdd.map(func).collect()
    print(sums)



