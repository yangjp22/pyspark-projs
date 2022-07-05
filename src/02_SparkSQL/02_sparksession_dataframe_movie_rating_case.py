# coding:utf8

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"


if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName('local[*]')\
        .config("spark.sql.shuffle.partitions", "4")\
        .getOrCreate()

    '''
    需求：
        1. 查询用户平均分
        2. 查询电影平均分
        3. 查询大于平均分的电影数量
        4. 查询高分电影中(>3)打分次数最多的用户，并求出此人打的平均分
        5. 查询每个用户的平均打分，最低打分，最高打分
        6. 查询被评分超过100次的电影的平均分 排名 取出top10
    数据：
        movielens.data (seperated by \t)
            User ID: 
            Movie ID:
            Ratings:
            TimeStamp:
    '''
    movie = spark.read.format("csv").\
        option("sep", "\t").\
        option("header", False).\
        option("encoding", "utf-8").\
        schema("UserID INTEGER, MovieID INTEGER, Rating INTEGER, TIME INTEGER").\
        load('../movielens.data')
    # movie.printSchema()
    # movie.show(20)

    # 1. 查询用户平均分
    # movie.groupby(movie['UserID']).avg('Rating').orderBy('avg(Rating)', ascending=False).show()

    # 2. 查询电影平均分
    # movie.groupby(movie['MovieID']).avg('Rating').orderBy('avg(Rating)', ascending=False).show()

    # # 3. 查询大于平均分的电影数量
    # avg = movie.select(F.avg(movie['Rating'])).first()['avg(Rating)']  # 取到行对象（Row）后再取列名
    # print(avg)
    # count = movie.filter(movie['Rating'] > avg).count()
    # print(count)

    # 4. 查询高分电影中(>3)打分次数最多的用户，并求出此人打的平均分
    # user_id = movie.filter(movie['Rating'] >= 3).\
    #     groupby(movie['UserID']).\
    #     count().\
    #     orderBy('count', ascending=False).\
    #     first()['UserID']
    # avg_rating = movie.groupby(movie['UserID']).\
    #     avg('Rating').\
    #     filter(movie['UserID'] == user_id).\
    #     first()['avg(Rating)']
    # print(avg_rating)

    # 5. 查询每个用户的平均打分，最低打分，最高打分
    # users_avg_rating = movie.groupby('UserID').\
    #     agg(
    #         F.round(F.avg('Rating'), 2).alias('avg_rating'),
    #         F.min('Rating').alias('min_rating'),
    #         F.max('Rating').alias('max_rating')
    #     )
    # users_avg_rating.show()

    # 6. 查询被评分超过100次的电影的平均分 排名 取出top10
    movie.groupby('MovieID').\
        agg(
            F.count('Rating').alias('cnt'),
            F.round(F.avg('Rating'), 2).alias('avg_rating')
        ).where("cnt>100").\
        orderBy('avg_rating', ascending=False).\
        limit(10).\
        show()
