# Use Apache Spark to perform the ETL Transform
# pip install pyspark
# pip instal findspark
# pyspark --version

import pandas as pd
import numpy as np
import findspark

# Initiating a spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, substring
from pyspark.sql.types import DateType, IntegerType


def transform():
    appName = "etl_tf"
    master = "local"

    # Create Spark session
    mySpark= SparkSession.builder.appName(appName).master(master).getOrCreate()

    # it's just a warning, and won't impact Hadoop's functionalities.
    mySpark.sparkContext.setLogLevel("WARN")
    # print the mySpart object
    print(mySpark)

    schema = 'Date_Time STRING, Store_Name STRING, Cust_Name STRING, Item STRING, Total DOUBLE, Payment STRING, Card_No STRING'
    # df = pd.read_csv('../data/chesterfield_25-08-2021_09-00-00.csv')
    # Use spark.read.option instead
    # df = mySpark.read.option('header', 'true').csv('../data/chesterfield_25-08-2021_09-00-00.csv')
    df = mySpark.read.csv('../data/chesterfield_25-08-2021_09-00-00.csv',schema=schema, header='True')

    df.write.csv('../db/part_1.csv', header=True)
    print(df.printSchema())
    df.show()

    # Create Spark session
    mySpark2= SparkSession.builder.appName(appName).master(master).getOrCreate()
    df1 = mySpark2.read.csv('../db/part_1.csv')
    # ------ Process the substring on Date_Time column
    df1.withColumn('Date_Time', substring('Date_Time',1,10))
    # df.select(substring(df.Date_Time, 1, 10).alias('txDate')).collect()

    #df.select(to_timestamp(df["Date_Time"], format="dd-mm-yyyy"))
    #df = df.withColumn("Date_Time", df["Date_Time"].cast(DateType()))
    df1.write.csv('../db/part_1.csv', header=True)



transform()