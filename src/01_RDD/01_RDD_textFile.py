# coding:utf8
from pyspark import SparkConf, SparkContext


if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]").setAppName("WordCountHelloWorld")
    # 通过SparkConf对象构建SparkContext对象
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("../word.txt")
    print(rdd.getNumPartitions())

    rdd2 = sc.textFile("hdfs:/.../word.txt")
    print(rdd2.getNumPartitions())
