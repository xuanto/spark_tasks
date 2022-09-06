# -*- coding:utf-8 -*-
"""
Copyright (c) 2021, Tencent Inc.
All Rights Reserved.
Author: (╯°□°）╯︵┻━┻) <(╯°□°）╯︵┻━┻)@tencent.com>
python env: python3

达成率报表脚本，计算新口径达成率并整合旧口径达成率，写入mysql DB
达成率报表建设文档：https://docs.qq.com/doc/DYXplcXdySlNoWEJN
"""
from pyspark import SparkContext, SQLContext
from pytoolkit import TDWProvider
from pytoolkit import TDWSQLProvider
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType, IntegerType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys


# magic numbers
DIM_ADGROUPID = 1
DIM_ADVERTISER = 2
INF_NUM = 999999.99

ACHIEVE_INTERVAL_MAP_COMMEN = {
    DIM_ADGROUPID:  {1: INF_NUM, 2: 1.0, 3: 0.6, 4: 0.2, 5: 0.1},
    DIM_ADVERTISER: {1: 0.1, 2: 0.1, 3: 0.1, 4: 0.1, 5: INF_NUM}
}

ACHIEVE_INTERVAL_MAP_EDU = {
    DIM_ADGROUPID:  {1: INF_NUM, 2: INF_NUM, 3: INF_NUM, 4: INF_NUM, 5: 0.1},
    DIM_ADVERTISER: {1: 0.1, 2: 0.1, 3: 0.1, 4: 0.1, 5: INF_NUM}
}
MYSQL_URL = \
    'jdbc:mysql://100.65.202.233:4183/reports?useSSL=false&useUnicode=true&characterEncoding=utf8'
DEBUG_FILE_PATH = "hdfs://ss-cdg-13-v2/user/tdw_(╯°□°）╯︵┻━┻)/tmp/reach_rate_analysis/"


@udf(returnType=IntegerType())
def get_active_num_interval(cost, target_cpa, click):
    """
    计算转化数区间，转化数区间为左闭右开，转化数采用目标转化数（目标转化数=消耗/转化出价）
    """
    if target_cpa <= 0 or click <= 0:
        return 0
    target_active_num = cost / target_cpa
    if (0 <= target_active_num < 1):
        return 1
    elif (1 <= target_active_num < 3):
        return 2
    elif (3 <= target_active_num < 5):
        return 3
    elif (5 <= target_active_num < 10):
        return 4
    elif (10 <= target_active_num):
        return 5
    return 0


@udf(returnType=DoubleType())
def get_achieve_interval(op_industry, active_interval, dim_type):
    """
    计算达成区间，返回达成区间的左右边界值（返回一个float）
    """
    if (op_industry == "education"):
        active_interval_map = ACHIEVE_INTERVAL_MAP_EDU[dim_type]
    else:
        active_interval_map = ACHIEVE_INTERVAL_MAP_COMMEN[dim_type]
    return active_interval_map.get(active_interval, 0.0)


@udf(returnType=IntegerType())
def get_success(achieve_interval, cpa_bias):
    """
    计算是否达成，达成广告返回1，否则返回0
    """
    if cpa_bias < 0:  # cpa_bias < 0 说明消耗为0
        return 0
    return 1 if cpa_bias < achieve_interval else 0


@udf(returnType=DoubleType())
def get_cpa_bias(gmv, cost):
    """
    计算成本偏差
    """
    if cost <= 0:
        return -1
    return abs(gmv / cost - 1)


@udf(returnType=StringType())
def conversion_bucket(conversions, cost, target_cpa):
    """
    根据转化数对数据进行分桶，spark只支持ascii编码，udf中不能有中文
    """
    if (conversions > 10) or (cost > 10 * target_cpa):
        return "over_10_conv"
    else:
        return "below_10_conv"


SQL_STR_TEMPLATE = """
SELECT
    partition_date,
    advertiser_industry AS op_industry,
    op_industry2,
    advertiser_id,
    sub_brand,
    product_id,
    adgroup_id,
    first_goal,
    second_goal,
    optimization_goal,
    SUM(costs) as costs,
    SUM(valid_click_cnt) as click,
    SUM(exposure_num) as exposure,
    SUM(gmv) as gmv,
    SUM(conversions) as conversions,
    SUM(case when second_goal = 0 then target_cpa1
            when second_goal > 1 then target_cpa2
            else 0 end) as target_cpa,
    SUM(conversions) * SUM(case
        when second_goal = 0 then target_cpa1
        when second_goal > 1 then target_cpa2
        else 0 end) as gmvc,
    SUM(case
        when second_goal = 0 then valid_click_pcvr_sum
        when second_goal > 1 then valid_click_pcvr2_sum
        else 0 end) as pcvr,
    SUM(valid_exposure_pctr_sum) / 1000000 as pctr,
    if(
      if(second_goal = 0, first_cpa_bias, second_cpa_bias) >= - (0.2 + 0.1 * use_amount_first)
        and if(second_goal = 0, first_cpa_bias, second_cpa_bias) <= (0.2 + 0.1 * use_amount_first),
      1, 0) as reach_rate_target,
    if(if(second_goal = 0, first_cpa_bias, second_cpa_bias) <= (0.2 + 0.1 * use_amount_first),
      1, 0) as below_rate_target
FROM
    (SELECT
        partition_date,
        b.industry_group as advertiser_industry,
        b.industry_team_level2 as op_industry2,
        b.op_sub_brand as sub_brand,
        advertiser_id,
        product_id,
        adgroup_id,
        smart_optimization_goal as first_goal,
        second_goal,
        (case when second_goal > 0 then second_goal
            else smart_optimization_goal
            end) as optimization_goal,
        SUM(real_cost_micros / 1000000) as costs,
        SUM(valid_click_cnt) as valid_click_cnt,
        SUM(valid_exposure_cnt) as exposure_num,
        SUM(gmv)/100  as gmv,
        SUM(active_num) as  conversion1,
        SUM(second_active_num)  as conversion2,
        MAX(if(deep_stage_status = 2, 1, 0)) as reach_double_phase,
        SUM(if(second_goal > 0, second_active_num, active_num)) as conversions,
        SUM(target_cpa_v2)/100/SUM(valid_exposure_cnt+valid_click_cnt) as target_cpa1,
        SUM(ocpa_second_target_cpa) / 100 / SUM(
            case when bid_type=1 then valid_click_cnt
                 when bid_type=4 then valid_exposure_cnt
                 else 0 end) as target_cpa2,
        SUM(adjusted_smart_pcvr / 1000000) AS valid_click_pcvr_sum,


        SUM(vc_adjusted_smart_pcvr2 / 1000000) AS valid_click_pcvr2_sum,
        SUM(valid_exposure_pctr / 1000000) AS valid_exposure_pctr_sum,
        MAX(if(bid_strategy = 2, 1, 0)) as use_amount_first,
        (SUM(real_cost_micros / 10000) / SUM(active_num)) / (SUM(valid_click_target_cpa) / SUM(valid_click_cnt)) - 1 AS first_cpa_bias,
        (SUM(real_cost_micros / 10000) / SUM(second_active_num)) / (SUM(vc_second_bid) / SUM(valid_click_cnt)) - 1 AS second_cpa_bias
    FROM ocpa_middle_table_d a
    JOIN t_ad_accounts_full_d b
    ON a.partition_date = b.imp_date and a.advertiser_id = b.account_id
        and a.site_set != 21
        and (a.no_compensation_type = 0 or a.no_compensation_type is null)
    WHERE a.smart_optimization_goal > 0
        and a.partition_date = %s
        and b.industry_group in ('金融', '教育', '家居', '房产', '大交通', '医药', '运营商','商务服务','招商加盟','旅游','本地生活')
    group by partition_date,
            b.industry_group,
            b.industry_team_level2,
            b.op_sub_brand,
            advertiser_id,
            product_id,
            adgroup_id,
            smart_optimization_goal,
            second_goal,
            (case when second_goal > 0 then second_goal else smart_optimization_goal end)
    )
where optimization_goal != 106
    and costs > 0
    and valid_click_cnt > 0
    and ((reach_double_phase = 1 and target_cpa2 > 0) or target_cpa1 > 0)
group by partition_date,
         advertiser_industry,
         advertiser_id,
         op_industry2,
         sub_brand,
         product_id,
         adgroup_id,
         first_goal,
         second_goal,
         optimization_goal,
         use_amount_first,
         first_cpa_bias,
         second_cpa_bias
"""


def load_data_from_tdw(spark, pri_parts_str) -> DataFrame:
    """
    从ams_industry2::achieve_rate_report_hist_data_d表中取其他报表的数据
    """
    provider = TDWSQLProvider(spark, db='ams_industry2')
    provider.table(
            'achieve_rate_report_hist_data_d', priParts = [pri_parts_str]
        ).createOrReplaceTempView('achieve_rate_report_hist_data_d')
    sql_str = """
        select CASE
            WHEN (op_industry in ('finance')) THEN '金融'
            WHEN (op_industry in ('education')) THEN '教育'
            WHEN (op_industry in ('property')) THEN '房产'
            WHEN (op_industry in ('furnishing')) THEN '家居'
            WHEN (op_industry in ('medicine')) THEN '医药'
            WHEN (op_industry in ('car')) THEN '大交通'
            WHEN (op_industry in ('yys')) THEN '运营商'
            WHEN (op_industry in ('zsfw')) THEN '商务服务'
            WHEN (op_industry in ('zsjm')) THEN '招商加盟'
            WHEN (op_industry in ('ly')) THEN '旅游'
            WHEN (op_industry in ('bdsh')) THEN '本地生活'
        END AS op_industry_cn, * from achieve_rate_report_hist_data_d"""
    mid_df = spark.sql(sql_str) \
                  .drop("op_industry") \
                  .withColumnRenamed("op_industry_cn", "op_industry")
    return mid_df


def get_joined_df(raw_df) -> DataFrame:
    """
    聚合数据，生成join后的dataframe
    """
    ac_cpa_bias_df = raw_df.groupby(["op_industry", "op_industry2", "t_advertiser"]) \
                           .agg(F.sum("costs").alias("costs"),
                                F.sum("gmvc").alias("gmvc")) \
                           .withColumn("ac_cpa_bias",
                                       get_cpa_bias(F.col("gmvc"), F.col("costs"))) \
                           .select("t_advertiser", "ac_cpa_bias") \
                           .cache()

    joined_df = raw_df.withColumn("active_interval",
                                  get_active_num_interval(F.col("costs"),
                                                          F.col("target_cpa"),
                                                          F.col("click"))) \
                      .withColumn("ad_achieve_interval",
                                  get_achieve_interval(F.col("op_industry"),
                                                       F.col("active_interval"),
                                                       F.lit(DIM_ADGROUPID))) \
                      .withColumn("ac_achieve_interval",
                                  get_achieve_interval(F.col("op_industry"),
                                                       F.col("active_interval"),
                                                       F.lit(DIM_ADVERTISER))) \
                      .withColumn("ad_cpa_bias",
                                  get_cpa_bias(F.col("gmvc"), F.col("costs"))) \
                      .withColumn("ad_cnt", F.lit(1)) \
                      .join(ac_cpa_bias_df, ["t_advertiser"], "left") \
                      .withColumn("success_adgroup_id",
                                  get_success(F.col("ad_achieve_interval"),
                                                  F.col("ad_cpa_bias"))) \
                      .withColumn("success_t_advertiser",
                                  get_success(F.col("ac_achieve_interval"),
                                                  F.col("ac_cpa_bias"))) \
                      .withColumn("success_account",
                                  get_success(F.lit(0.1), F.col("ac_cpa_bias"))) \
                      .withColumn("achieve",
                                  F.col("success_adgroup_id") * F.col("success_t_advertiser")) \
                      .withColumn("achieve_cost", F.col("costs") * F.col("achieve")) \
                      .withColumn("ad_reach_cost", F.col("costs") * F.col("success_adgroup_id")) \
                      .withColumn("ac_reach_cost",
                                  F.col("costs") * F.col("success_account")) \
                      .withColumn("active_bucket", conversion_bucket(F.col("conversions"),
                                                                     F.col("costs"),
                                                                     F.col("target_cpa"))) \
                      .cache()
    print(joined_df.columns)
    return joined_df


def caculate_table_value(df, group_list, col_list) -> DataFrame:
    """
    输入聚合后的dataframe，根据 group_list 分组之后输出 col_list 中的指标
    """
    res_df = df.groupby(group_list) \
               .agg(F.sum("achieve_cost").alias("achieve_cost"),
                    F.sum("ad_reach_cost").alias("ad_reach_cost"),
                    F.sum("ac_reach_cost").alias("ac_reach_cost"),
                    F.sum("reach_cost").alias("reach_cost"),
                    F.sum("below_cost").alias("below_cost"), 
                    F.sum("costs").alias("cost"),
                    F.sum("click").alias("click_num"),
                    F.sum("exposure").alias("exposure"),
                    F.sum("conversions").alias("conversion_num"),
                    F.sum("target_cpa").alias("target_cpa"),
                    F.sum("gmvc").alias("gmv"),
                    F.sum("pcvr").alias("vc_pcvr"),
                    F.sum("pctr").alias("ve_pctr"),
                    F.sum("ad_cnt").alias("ad_num")) \
               .withColumn("achieve_rate", F.col("achieve_cost") / F.col("cost")) \
               .withColumn("ad_reach_rate", F.col("ad_reach_cost") / F.col("cost")) \
               .withColumn("ac_reach_rate", F.col("ac_reach_cost") / F.col("cost")) \
               .withColumn("cvr", F.col("conversion_num") / F.col("click_num")) \
               .withColumn("pcvr", F.col("vc_pcvr") / F.col("click_num")) \
               .withColumn("pcvr_bias", F.col("vc_pcvr") / F.col("conversion_num") - F.lit(1)) \
               .withColumn("ctr", F.col("click_num") / F.col("exposure")) \
               .withColumn("pctr", F.col("ve_pctr") / F.col("exposure")) \
               .withColumn("pctr_bias", F.col("ve_pctr") / F.col("click_num") - F.lit(1)) \
               .withColumn("reach_rate", F.col("reach_cost") / F.col("cost")) \
               .withColumn("below_rate", F.col("below_cost") / F.col("cost")) \
               .withColumn("gmv_to_cost", F.col("gmv") / F.col("cost")) \
               .select(*col_list) \
               .cache()
    return res_df


def save_df_into_mysqldb(df, table_name):
    """
    将dataframe存入mysqlDB的reports库
    mode 设为 overwrite 保证表中数据皆为最新数据
         设为 append    表中保留历史数据
    """
    auth_mysql = {"user": "user_00", "password": "isd!@#user"}
    df.write.jdbc(MYSQL_URL, table_name, mode="append", properties=auth_mysql)
    return


if __name__ == '__main__':
    sc = SparkContext(appName="achieve_rate_new_report")
    spark = SparkSession.builder.appName("achieve_rate_new_report").getOrCreate()
    end_date = sys.argv[1]
    # 1.老达成率存表 2.计算新达成率 4.转化数分桶 8.子品牌聚合 15.全都要
    # 16.子品牌转化数分桶  32.写debug文件
    magic_code = int(sys.argv[2])
    save_to_db = False if (len(sys.argv) > 3 and sys.argv[3] == "no_save") else True
    sub_brand_list = sys.argv[4] if (len(sys.argv) > 4) else ""
    sql_sub_brand = "sub_brand in ('%s')" % "','".join(sub_brand_list.split(","))
    print(sql_sub_brand)
    pri_parts_str = "p_" + end_date
    print(end_date)

    # 从ocpa中间表中获取数据
    provider = TDWSQLProvider(spark, db='hlw_gdt')
    provider.table(
            't_ocpa_middle_table_d', priParts=[pri_parts_str]
        ).createOrReplaceTempView('ocpa_middle_table_d')
    provider2 = TDWSQLProvider(spark, db='ams_access_db')
    provider2.table(
            't_ad_accounts_full_d', priParts=[pri_parts_str]
        ).createOrReplaceTempView('t_ad_accounts_full_d')
    sql_str = SQL_STR_TEMPLATE % (end_date)
    raw_df = spark.sql(sql_str) \
                  .na.drop(subset=["op_industry", "product_id", "optimization_goal"]) \
                  .withColumn("t_advertiser", F.concat_ws('_',
                                                          F.col("advertiser_id"),
                                                          F.col("product_id"),
                                                          F.col("optimization_goal"))) \
                  .withColumn("reach_cost", F.col("reach_rate_target") * F.col("costs")) \
                  .withColumn("below_cost", F.col("below_rate_target") * F.col("costs")) \
                  .cache()

    joined_df = get_joined_df(raw_df)
    loan_df = joined_df.where(F.col("op_industry2") == "贷款").cache()

    if ((magic_code & 32) != 0):
        print("output debug file ...")
        print(joined_df.columns)
        joined_df.filter("sub_brand in ('分期乐', '马上消费金融')") \
            .write.format("csv") \
            .option("header","false") \
            .mode("overwrite") \
            .save(DEBUG_FILE_PATH + end_date + "_joined_df")
        print(DEBUG_FILE_PATH)

    ################################################################
    #  magic_code = 1  计算老达成率并存表  此逻辑后续可拆分出去
    ################################################################
    if ((magic_code & 1) != 0):
        tdw_df = load_data_from_tdw(spark, pri_parts_str)  # 从tdw导入1、2口径的数据
        if save_to_db:
            save_df_into_mysqldb(tdw_df, "achieve_rate_multi_trace_d")

    common_col_list = ["ad_num", "achieve_rate", "ad_reach_rate", "ac_reach_rate",
                       "cost", "gmv", "click_num", "conversion_num",
                       "pcvr", "cvr", "pcvr_bias", "pctr", "ctr", "pctr_bias"]

    ################################################################
    #  magic_code = 2  计算新达成率并存表
    ################################################################
    if ((magic_code & 2) != 0):
        col_list = ["partition_date", "op_industry"] + common_col_list
        group_list = ["partition_date", "op_industry"]
        final_df = caculate_table_value(joined_df, group_list, col_list)

        # 对贷款行业单独计算
        col_list_loan = ["partition_date", "op_industry2"] + common_col_list
        group_list_loan = ["partition_date", "op_industry2"]
        loan_final_df = caculate_table_value(loan_df, group_list_loan, col_list_loan)
        loan_final_df = loan_final_df.withColumn("op_industry", F.lit("贷款")).select(*col_list)

        final_df = final_df.union(loan_final_df)
        if save_to_db:
            save_df_into_mysqldb(final_df, "new_reach_rate_report_d")
        else:
            final_df.show()

    ################################################################
    #  magic_code = 4  按照转化数分桶计算新达成率
    ################################################################
    if ((magic_code & 4) != 0):
        col_list = ["partition_date", "op_industry", "active_bucket"] + \
                common_col_list + ["reach_rate"]
        group_list = ["partition_date", "op_industry", "active_bucket"]
        bucket_df = caculate_table_value(joined_df, group_list, col_list)

        # 对贷款行业单独计算
        col_list_loan = ["partition_date", "op_industry2", "active_bucket"] + \
                common_col_list + ["reach_rate"]
        group_list_loan = ["partition_date", "op_industry2", "active_bucket"]
        loan_bucket_df = caculate_table_value(loan_df, group_list_loan, col_list_loan)
        loan_bucket_df = loan_bucket_df.withColumn("op_industry", F.lit("贷款")).select(*col_list)

        bucket_df = bucket_df.union(loan_bucket_df) \
                             .drop("ad_reach_rate") \
                             .withColumnRenamed("reach_rate", "ad_reach_rate")
        if save_to_db:
            save_df_into_mysqldb(bucket_df, "industry_conv_bucket_new_reach_rate_d")
        else:
            bucket_df.show()

    ################################################################
    #  magic_code = 8  按照子品牌聚合计算新达成率
    ################################################################
    if ((magic_code & 8) != 0):
        col_list = ["partition_date", "op_industry2", "sub_brand"] \
                + common_col_list + ["reach_rate", "below_rate"]
        group_list = ["partition_date", "op_industry2", "sub_brand"]
        sub_brand_df = caculate_table_value(joined_df, group_list, col_list)
        print((sub_brand_df.count(), len(sub_brand_df.columns)))
        sub_brand_df = sub_brand_df.filter(sql_sub_brand)
        print((sub_brand_df.count(), len(sub_brand_df.columns)))
        if save_to_db:
            save_df_into_mysqldb(sub_brand_df.orderBy(F.desc("cost")),
                                 "vip_sub_brand_new_reach_rate_d")

    ################################################################
    #  magic_code = 16  按照子品牌x转化数分桶聚合计算新达成率
    #（目前未使用此报表，当magic_code=16时，save应为no）
    ################################################################
    if ((magic_code & 16) != 0):
        col_list = ["partition_date", "op_industry2", "sub_brand", "active_bucket"] \
                + common_col_list + ["reach_rate", "below_rate", "gmv_to_cost"]
        group_list = ["partition_date", "op_industry2", "sub_brand", "active_bucket"]
        sub_brand_df = caculate_table_value(joined_df, group_list, col_list)
        print((sub_brand_df.count(), len(sub_brand_df.columns)))
        sub_brand_df = sub_brand_df.filter(sql_sub_brand)
        print((sub_brand_df.count(), len(sub_brand_df.columns)))
        if save_to_db:
            save_df_into_mysqldb(sub_brand_df.orderBy(F.desc("cost")),
                                 "vip_sub_brand_bucket_reach_rate_d")

    print("all works done!")
    spark.stop()
