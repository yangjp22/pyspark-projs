# coding:utf8

from pyspark import SparkContext, SparkConf
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    abnormalChar = [",", ".", "!", "#", "$", "%"]
    broadcast = sc.broadcast(abnormalChar)

    rdd = sc.textFile('../case.txt')
    words = rdd.flatMap(lambda x: x.strip().split(" "))

    words_c = words.map(lambda x: (x, 1))
    groupWords = words_c.reduceByKey(lambda a, b: a + b)
    groupWords.cache()

    # 累加器计算特殊字符和正常单词的个数
    normal = sc.accumulator(0)
    abnormal = sc.accumulator(0)
    def cal(word):
        global normal, abnormal
        if word in broadcast.value:
            abnormal += 1
        else:
            normal += 1
    words.map(cal).collect()
    print("abnormal word count: ", abnormal)
    print("normal word count: ", normal)

    # 筛选出正常正常单词的个数
    print(groupWords.collect())
    def filters(item):
        if not item[0] or item[0] in broadcast.value:
            return False
        return True
    print(groupWords.filter(filters).collect())
