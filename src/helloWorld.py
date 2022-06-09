# coding:utf8
from pyspark import SparkConf, SparkContext

def tryPartition(sc):
    rdd = sc.parallelize([1, 5, 3, 3, 4, 5], 3)
    return rdd.glom().collect()


if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]").setAppName("WordCountHelloWorld")
    # 通过SparkConf对象构建SparkContext对象
    sc = SparkContext(conf=conf)

    print(tryPartition(sc))
    # 需求：wordcount单词计数，读取HDFS上的word.txt文件，对其内部的单词统计出现的数量
    # fileRdd = sc.textFile("./word.txt")
    # # 对单词进行切割，得到一个存储全部单词的集合对象
    # wordsRdd = fileRdd.flatMap(lambda line: line.split(" "))
    # # 将单词转换为元祖对象，key是单词，value是数字1
    # wordsWithOneRdd = wordsRdd.map(lambda x: (x, 1))
    # # 将元祖的value 按照key来分组，对所有相同key的value执行聚合操作（相加）
    # resultRdd = wordsWithOneRdd.reduceByKey(lambda a, b: a + b)
    # # 通过collect方法收集RDD的数据 打印输出结果
    # print(resultRdd.collect())





