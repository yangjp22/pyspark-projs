# coding:utf8

from pyspark import SparkConf, SparkContext


if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    data = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    rdd = sc.parallelize(data)
    print(rdd.getNumPartitions())