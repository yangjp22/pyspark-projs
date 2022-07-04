# coding:utf8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import os

os.environ["PYSPARK_PYTHON"] = "../../venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "../../venv/bin/python"


if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName('local[*]')\
        .config("spark.sql.shuffle.partitions", "4")\
        .getOrCreate()

    sc = spark.sparkContext

    rdd = sc.textFile("../stu_score.txt").\
        map(lambda x: x.split(",")).\
        map(lambda x: [int(x[0]), x[1], int(x[2])])

    # 法一、
    df = spark.createDataFrame(rdd, schema=['ID', 'name', 'score'])
    # df.printSchema()
    # df.show(20, False)
    # df.createTempView('temp')
    # spark.sql('''
    #     select * from temp where name="语文"
    # ''').show()

    # 法二、
    schema = StructType().\
        add("id", IntegerType(), nullable=False).\
        add("subject", StringType(), nullable=True).\
        add("score", IntegerType(), nullable=False)
    df2 = spark.createDataFrame(rdd, schema=schema)
    df2.show()

    # 法三、
    df3 = rdd.toDF(['id', 'subject', 'grade'])
    df3.show()

    # 法四
    df4 = rdd.toDF(schema=schema)
    df4.show()

    # 从pandas中的DataFrame创建
    # pd_df = pandas.DataFrame(...)
    # spark.createDataFrame(pd_df)















