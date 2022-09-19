# -*- coding:utf-8 -*-
"""
Copyright (c) 2021, █████ Inc.
All Rights Reserved.
Author: █████ <█████@█████.com>
医药行业达成率报表脚本，拆分子行业，到达口径，写入mysql DB
达成率报表建设文档：████████████████████
"""
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
    sql_str = "select * from finance_edu_action_bucket_reach_rate_d where op_industry='医药'"
    mid_df = spark.sql(sql_str)
    return mid_df


def save_df_into_mysqldb(df, table_name):
    """
    将dataframe存入mysqlDB给黄金眼使用,
    mode 设为 overwrite 保证表中数据皆为最新数据
         设为 append    表中保留历史数据
    """
    url = "jdbc:mysql://█████.█████.█████.233:4183/reports" + \
            "?useSSL=false&useUnicode=true&characterEncoding=utf8"
    auth_mysql = {"user": "█████", "password": "█████"}
    df.write.jdbc(url, table_name, mode="append", properties=auth_mysql)
    return


SQL_STR_TEMPLATE = """
SELECT
    partition_date,
    op_industry2,
    COUNT(*) as ad_num,
    SUM(real_cost_sum * reach_rate_target) / SUM(real_cost_sum) as achieve_rate,
    SUM(real_cost_sum * below_rate_target) / SUM(real_cost_sum) as below_rate,
    SUM(real_cost_sum * kong_rate_0) / SUM(real_cost_sum) as ec_rate_0,
    SUM(real_cost_sum * kong_rate_2) / SUM(real_cost_sum) as ec_rate_2,
    SUM(real_cost_sum) as cost,
    SUM(GMV) as GMV,
    SUM(click_num) as click_num,
    SUM(conversion_num) as conversion_num,
    SUM(valid_click_pcvr_sum) / SUM(click_num) as pcvr,
    SUM(conversion_num) / SUM(click_num) cvr,
    SUM(valid_click_pcvr_sum) / SUM(click_num) / (SUM(conversion_num) / SUM(click_num)) - 1 as pcvr_bias,
    SUM(valid_exposure_pctr_sum) / 1000000 / SUM(exposure_num) as pctr,
    SUM(click_num) / SUM(exposure_num) as ctr,
    SUM(valid_exposure_pctr_sum) / 1000000 / SUM(exposure_num) / (SUM(click_num) / SUM(exposure_num)) - 1 as pctr_bias,
    SUM(gsp_factor * click_num) / SUM(click_num) as gsp_factor,
    SUM(real_gsp_factor * click_num) / SUM(click_num) as real_gsp_factor
FROM (
    SELECT
        partition_date,
        adgroup_id,
        1 AS process_gap,
        op_industry2,
        real_cost_sum,
        if(second_goal = 0, GMV, GMV2) as GMV,
        exposure_num,
        click_num,
        if(second_goal = 0, conversion_num, second_conversion_num) as conversion_num,
        if(if(second_goal = 0, first_cpa_bias, second_cpa_bias) >= - (0.2 + 0.1 * use_amount_first) and if(second_goal = 0, first_cpa_bias, second_cpa_bias) <= (0.2 + 0.1 * use_amount_first), 1, 0) as reach_rate_target,
        if(if(second_goal = 0, first_cpa_bias, second_cpa_bias) <= (0.2 + 0.1 * use_amount_first), 1, 0) as below_rate_target,
        if(if(second_goal = 0, conversion_num, second_conversion_num) == 0, 1, 0) as kong_rate_0,
        if(if(second_goal = 0, conversion_num, second_conversion_num) <= 2, 1, 0) as kong_rate_2,
        if(second_goal = 0, valid_click_pcvr_sum, valid_click_pcvr2_sum) as valid_click_pcvr_sum,
        valid_exposure_pctr_sum,
        gsp_factor,
        real_gsp_factor
    FROM (
        SELECT
            partition_date,
            adgroup_id,
            industry_team_level2 as op_industry2,
            second_goal,
            MAX(if(bid_strategy = 2, 1, 0)) as use_amount_first,
            MAX(if(deep_stage_status = 2, 1, 0)) as reach_double_phase,
            SUM(valid_exposure_cnt) as exposure_num,
            SUM(valid_click_cnt) AS click_num,
            (SUM(real_cost_micros) / 10000 / SUM(active_num)) / (SUM(valid_click_target_cpa) / SUM(valid_click_cnt)) - 1 AS first_cpa_bias,
            (SUM(real_cost_micros) / 10000 / SUM(second_active_num)) / (SUM(vc_second_bid) / SUM(valid_click_cnt)) - 1 AS second_cpa_bias,
            SUM(real_cost_micros) / 1000000 AS real_cost_sum,
            SUM(active_num) AS conversion_num,
            SUM(second_active_num) as second_conversion_num,
            SUM(valid_click_target_cpa) AS first_target_cpa_sum,
            SUM(vc_second_bid) AS second_target_cpa_sum,
            SUM(gmv) / 100 as GMV,
            SUM(second_gmv) / 100 as GMV2,
            SUM(adjusted_smart_pcvr) / 1000000 AS valid_click_pcvr_sum,
            SUM(vc_adjusted_smart_pcvr2) / 1000000 AS valid_click_pcvr2_sum,
            SUM(valid_exposure_pctr) / 1000000 AS valid_exposure_pctr_sum,
            SUM(ocpa_gsp_factor) / SUM(if(bid_type = 4, valid_exposure_cnt, valid_click_cnt)) as gsp_factor,
            SUM(valid_gsp_exposure_gsp_factor) / SUM(valid_gsp_exposure_cnt) as real_gsp_factor
        FROM t_ocpa_middle_table_d a
        join t_ad_accounts_full_d b
        on a.partition_date = b.imp_date and a.advertiser_id = b.account_id
          and a.partition_date = %s
          and a.site_set != 21
          and (not (a.no_compensation_type != 0 and a.no_compensation_type is not null))
        WHERE b.industry_group in ('医药') and second_goal != 106
        GROUP BY partition_date, adgroup_id, industry_team_level2, second_goal
        ) ad_base_info
    WHERE real_cost_sum > 0
        and click_num > 0
        and ((reach_double_phase = 1 and second_target_cpa_sum > 0) or first_target_cpa_sum > 0)
    ) ad_reach_info
GROUP BY partition_date, op_industry2
"""


if __name__ == '__main__':
    sc = SparkContext(appName="medicine_reach_rate_report")
    spark = SparkSession.builder.appName("medicine_reach_rate_report").getOrCreate()
    end_date = sys.argv[1]
    pri_parts_str = "p_" + end_date
    print(end_date)

    # 医药分桶报表
    tdw_df = load_data_from_tdw(spark, pri_parts_str)
    table_name = 'medicine_sub_industry_conv_bucket_reach_rate_d'
    save_df_into_mysqldb(tdw_df, table_name)
    print(table_name, " is ok !!")

    # 医药分子行业报表
    provider = TDWSQLProvider(spark, db='hlw_gdt')
    provider.table('t_ocpa_middle_table_d', priParts=[pri_parts_str]
        ).createOrReplaceTempView('t_ocpa_middle_table_d')
    provider2 = TDWSQLProvider(spark, db='ams_access_db')
    provider2.table('t_ad_accounts_full_d', priParts=[pri_parts_str]
        ).createOrReplaceTempView('t_ad_accounts_full_d')
    sql_str = SQL_STR_TEMPLATE % (end_date)
    print(sql_str)
    mid_df = spark.sql(sql_str)
    table_name = 'medicine_sub_industry_reach_rate_d'
    save_df_into_mysqldb(mid_df, table_name)
    print(table_name, " is ok !!")

    spark.stop()
