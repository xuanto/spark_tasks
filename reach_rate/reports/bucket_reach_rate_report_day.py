# -*- coding:utf-8 -*-
"""
Copyright (c) 2021, █████ Inc.
All Rights Reserved.
Author: █████ <█████@█████.com>
达成率报表脚本，计算新口径达成率并整合旧口径达成率，写入mysql DB
达成率报表建设文档：████████████████████
"""
from __future__ import print_function

from pyspark import SparkContext, SQLContext
from pytoolkit import TDWProvider
from pytoolkit import TDWSQLProvider
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType, IntegerType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys


def load_data_from_tdw(spark, pri_parts_str):
    """
    从ams_industry2::finance_edu_action_bucket_reach_rate_d
    """
    provider = TDWSQLProvider(spark, db='ams_industry2')
    provider.table(
            'finance_edu_action_bucket_reach_rate_d', priParts = [pri_parts_str]
        ).createOrReplaceTempView('finance_edu_action_bucket_reach_rate_d')
    sql_str = "select * from finance_edu_action_bucket_reach_rate_d"
    mid_df = spark.sql(sql_str)
    return mid_df


def save_df_into_mysqldb(df):
    """
    将dataframe存入mysqlDB给黄金眼使用,
    mode 设为 overwrite 保证表中数据皆为最新数据
         设为 append    表中保留历史数据
    """
    url = 'jdbc:mysql://100.65.202.233:4183/reports?useSSL=false&useUnicode=true&characterEncoding=utf8'
    table = 'finance_edu_action_bucket_reach_rate_analysis'
    auth_mysql = {"user": "user_00", "password": "isd!@#user"}
    df.write.jdbc(url, table, mode="append", properties=auth_mysql)
    return


if __name__ == '__main__':
    sc = SparkContext(appName="bucket_reach_rate_report_day")
    spark = SparkSession.builder.appName("bucket_reach_rate_report_day").getOrCreate()
    end_date = sys.argv[1]
    pri_parts_str = "p_" + end_date
    print(end_date)

    tdw_df = load_data_from_tdw(spark, pri_parts_str)
    save_df_into_mysqldb(tdw_df)
    spark.stop()
