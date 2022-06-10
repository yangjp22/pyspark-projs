# coding:utf8

from pyspark import SparkContext, SparkConf
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"


if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 构建RDD
    rdd = sc.textFile("../word.txt")
    print(rdd.getNumPartitions())
    print(rdd.collect())

    rdd_line_words = rdd.flatMap(lambda x: x.split(" "))
    print(rdd_line_words.getNumPartitions())
    print(rdd_line_words.collect())

    rdd_line_word_counts = rdd_line_words.map(lambda x: (x, 1))
    print(rdd_line_word_counts.collect())

    rdd_word_counts = rdd_line_word_counts.reduceByKey(lambda x, y: x + y)
    print(rdd_word_counts.getNumPartitions())
    print(rdd_word_counts.collect())

    rdd_word_counts_triple = rdd_word_counts.mapValues(lambda x: x * 3)
    print(rdd_word_counts_triple.collect())
