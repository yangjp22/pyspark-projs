# coding:utf8

from pyspark import SparkContext, SparkConf
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    studentInfo = [(1, '张大仙', 11),
                   (2, '王晓晓', 13),
                   (3, '张甜甜', 11),
                   (4, '王大力', 11)]

    # 将本地python list对象标记为广播变量
    broadcast = sc.broadcast(studentInfo)

    scoreInfoRdd = sc.parallelize([
        (1, '语文', 99),
        (2, '数学', 99),
        (3, '英语', 99),
        (4, '编程', 99),
        (1, '语文', 99),
        (2, '数学', 99),
        (3, '英语', 99),
        (4, '编程', 99),
        (1, '语文', 99),
        (2, '数学', 99),
        (3, '英语', 99),
        (4, '编程', 99),
    ])

    def mapFunc(data):
        idx = data[0]
        name = ""
        # 匹配本地list和分布式rdd中学生ID 匹配成功后 即可以获取当前学生的姓名
        # 在原本需要使用本地集合对象的地方，从广播变量中取出来用
        # for stuInfo in studentInfo:
        for stuInfo in broadcast.value:
            stuid = stuInfo[0]
            if stuid == idx:
                name = stuInfo[1]
        return (name, data[1], data[2])

    print(scoreInfoRdd.map(mapFunc).collect())

"""
使用场景： 本地集合对象 和 分布式集合对象（RDD） 进行关联的时候
需要将本地集合对象 封装从broadcast变量
可以节省：
    网络IO的次数
    Executor的内存占用
"""
