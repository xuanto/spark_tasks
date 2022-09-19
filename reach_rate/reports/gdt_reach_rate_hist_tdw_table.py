# coding: utf-8
# author █████
#
# 针对各行业重点优化目标（第一第二目标），统计其达成率，不超成本率等信息

from datetime import datetime, timedelta

TDW_RES_DBNAME = 'ams_industry2'
TDW_RES_TABLENAME = 'achieve_rate_report_hist_data_d'
SQL_MID_TEMPLATE = """op_industry,
            COUNT(*) as ad_num,
            SUM(real_cost_sum * reach_rate_target) / SUM(real_cost_sum) as reach_rate_target_cpa,
            SUM(real_cost_sum * below_rate_target) / SUM(real_cost_sum) as below_rate_target_cpa,
            SUM(real_cost_sum * kong_rate_0) / SUM(real_cost_sum) as kong_rate_0,
            SUM(real_cost_sum * kong_rate_2) / SUM(real_cost_sum) as kong_rate_2,
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
                adgroup_id,
                CASE
                    WHEN (op_industry in ('游戏', '综合电商','垂直电商')) THEN 3
                    WHEN (op_industry in ('金融')) THEN 2
                    WHEN (op_industry in ('教育', '家居', '房产', '大交通', '医药', '运营商','商务服务','招商加盟','旅游','本地生活')) THEN 1
                END AS process_gap,
                CASE
                    WHEN (op_industry in ('金融')) THEN 'finance'
                    WHEN (op_industry in ('教育')) THEN 'education'
                    WHEN (op_industry in ('房产')) THEN 'property'
                    WHEN (op_industry in ('家居')) THEN 'furnishing'
                    WHEN (op_industry in ('医药')) THEN 'medicine'
                    WHEN (op_industry in ('大交通')) THEN 'car'
                    WHEN (op_industry in ('运营商')) THEN 'yys'
                    WHEN (op_industry in ('商务服务')) THEN 'zsfw'
                    WHEN (op_industry in ('招商加盟')) THEN 'zsjm'
                    WHEN (op_industry in ('旅游')) THEN 'ly'
                    WHEN (op_industry in ('本地生活')) THEN 'bdsh'
                END AS op_industry,
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
                    adgroup_id,
                    industry_group as op_industry,
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
                FROM hlw_gdt::t_ocpa_middle_table_d a
                join ams_access_db::t_ad_accounts_full_d b"""


def NextPartitionTime(strDateTime, gap):
    if len(strDateTime) == 8:
        offset = timedelta(days=gap)
        statDate = datetime.strptime(strDateTime, '%Y%m%d')
        return (statDate + offset).strftime('%Y%m%d')
    elif len(strDateTime) == 10:
        offset = timedelta(hours=gap)
        statDate = datetime.strptime(strDateTime, '%Y%m%d%H')
        return (statDate + offset).strftime('%Y%m%d%H')
    raise RuntimeError('time format not support ' + str(strDateTime))


def AddPartition(tdw, tableName, strDateTime):
    partitionDate = NextPartitionTime(strDateTime, 1)
    adSql = 'ALTER TABLE %s ADD PARTITION p_%s VALUES LESS THAN (%s)' % \
        (tableName, strDateTime, partitionDate)
    try:
        tdw.execute(adSql)
    except Exception as e:
        tdw.WriteLog('add partition error message:' + e.message)
        tdw.WriteLog('add partition failed! SQL is:' + adSql)


def TDW_PL(tdw, argv):
    """
    argv: ['20191208']
    argvDate = "20210520"
    """
    argvDate = '%s' % argv[0]
    tableName = TDW_RES_TABLENAME

    tdw.execute("use %s" % TDW_RES_DBNAME)
    AddPartition(tdw, tableName, argvDate)

    SQL = """
INSERT
    TABLE %(tableName)s
        SELECT
            %(argvDate)s,
            date_sub(%(argvDate)s, MAX(process_gap)) as process_date,
            1,
            %(mid_sql)s
                on a.process_date = b.imp_date and a.advertiser_id = b.account_id
                    and a.partition_date > date_sub(%(argvDate)s, 1 + 3)
                    and a.partition_date <= %(argvDate)s
                    and a.process_date > date_sub(%(argvDate)s, 1 + 3)
                    and a.process_date <= date_sub(%(argvDate)s, 1)
                    and a.site_set != 21
                    and (not (a.no_compensation_type != 0 and a.no_compensation_type is not null))
                    and second_goal != 106
                WHERE
                    ((b.industry_group in ('金融')
                            and a.partition_date > date_sub(%(argvDate)s, 1 + 2)
                            and a.process_date > date_sub(%(argvDate)s, 1 + 2)
                            and a.process_date <= date_sub(%(argvDate)s, 1 + 1))
                        or (b.industry_group in ('教育', '家居', '房产', '大交通', '医药', '运营商','商务服务','招商加盟','旅游','本地生活')
                            and a.partition_date > date_sub(%(argvDate)s, 1 + 1)
                            and a.process_date > date_sub(%(argvDate)s, 1 + 1)
                            and a.process_date <= date_sub(%(argvDate)s, 1)))
                GROUP BY adgroup_id, industry_group, second_goal) ad_base_info
            WHERE real_cost_sum > 0
                and click_num > 0
                and ((reach_double_phase = 1 and second_target_cpa_sum > 0) or first_target_cpa_sum > 0)
            ) ad_reach_info
        GROUP BY op_industry
    """ % {"tableName": tableName, "argvDate": argvDate, "mid_sql": SQL_MID_TEMPLATE}

    tdw.WriteLog('SQL:' + SQL)
    tdw.execute(SQL)

    SQL = """
INSERT
    TABLE %(tableName)s
        SELECT
            %(argvDate)s,
            %(argvDate)s as process_date,
            2,
            %(mid_sql)s
                on a.partition_date = b.imp_date and a.advertiser_id = b.account_id
                  and a.partition_date = %(argvDate)s
                  and a.site_set != 21
                  and (not (a.no_compensation_type != 0 and a.no_compensation_type is not null))
                WHERE b.industry_group in ('金融', '教育', '家居', '房产', '大交通', '医药', '运营商','商务服务','招商加盟','旅游','本地生活')
                      and second_goal != 106
                GROUP BY adgroup_id, industry_group, second_goal) ad_base_info
            WHERE real_cost_sum > 0
                and click_num > 0
                and ((reach_double_phase = 1 and second_target_cpa_sum > 0) or first_target_cpa_sum > 0)
            ) ad_reach_info
        GROUP BY op_industry
    """ % {"tableName": tableName, "argvDate": argvDate, "mid_sql": SQL_MID_TEMPLATE}

    tdw.WriteLog('SQL:' + SQL)
    tdw.execute(SQL)
