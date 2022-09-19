# coding: utf-8
# author █████
#
# MP达成报表，针对各行业重点优化目标（第一第二目标），统计其达成率，不超成本率等信息

from datetime import datetime, timedelta


TDW_RES_DBNAME = 'ams_industry2'
TDW_RES_TABLENAME = 'mp_reach_rate_report_hist_data_d'
TDW_RES_TABLENAME_GDT = 'achieve_rate_report_hist_data_d'
SQL_MID_TEMPLATE = """op_industry,
            SUM(if(real_cost_sum > 0, 1, 0)) as ad_num,
            SUM(real_cost_sum * reach_rate_target) / SUM(real_cost_sum) as reach_rate_target_cpa,
            SUM(real_cost_sum * below_rate_target) / SUM(real_cost_sum) as below_rate_target_cpa,
            SUM(real_cost_sum * kong_rate_0) / SUM(real_cost_sum) as kong_rate_0,
            SUM(real_cost_sum * kong_rate_2) / SUM(real_cost_sum) as kong_rate_2,
            SUM(real_cost_sum) as cost,
            SUM(gmv) as gmv,
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
                gmv,
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
                    second_optimization_goal as second_goal,
                    MAX(if(ocpa_bid_strategy = 2, 1, 0)) as use_amount_first,
                    SUM(valid_exposure_cnt) as exposure_num,
                    SUM(valid_click_cnt) AS click_num,
                    (SUM(real_cost_micros / 10000) / SUM(ocpx_conversion_cnt)) / (SUM(vc_target_cpa) / SUM(valid_click_cnt)) - 1 AS first_cpa_bias,
                    (SUM(real_cost_micros / 10000) / SUM(second_conversion_cnt)) / (SUM(vc_second_target_cpa) / SUM(valid_click_cnt)) - 1 AS second_cpa_bias,
                    SUM(real_cost_micros / 1000000) AS real_cost_sum,
                    SUM(ocpx_conversion_cnt) AS conversion_num,
                    SUM(second_conversion_cnt) as second_conversion_num,
                    SUM(vc_target_cpa) AS first_target_cpa_sum,
                    SUM(vc_second_target_cpa) AS second_target_cpa_sum,
                    SUM(gmv_exp_d / 1000000) as gmv,
                    SUM(vc_adjusted_smart_pcvr) / 1000000 AS valid_click_pcvr_sum,
                    SUM(vc_adjusted_smart_pcvr2) / 1000000 AS valid_click_pcvr2_sum,
                    SUM(ve_pctr) / 1000000 AS valid_exposure_pctr_sum,
                    SUM(ve_or_vc_gsp_factor) / SUM(if(bid_type = 4, valid_exposure_cnt, valid_click_cnt)) as gsp_factor,
                    SUM(valid_gsp_exposure_gsp_factor) / SUM(valid_gsp_exposure_cnt) as real_gsp_factor
                FROM ams_data_warehouse::t_report_ad_d a
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
    # argvDate = '20211120'
    argvDate = '%s' % argv[0]
    is_mp = True if argv[1] == "mp" else False
    tableName = TDW_RES_TABLENAME if is_mp else TDW_RES_TABLENAME_GDT
    mp_str = "=" if is_mp else "!="

    tdw.execute("use %s" % TDW_RES_DBNAME)
    AddPartition(tdw, tableName, argvDate)

    SQL = """
INSERT
    TABLE %(tableName)s
        SELECT
            %(argvDate)s,
            date_sub(%(argvDate)s, MAX(process_gap)) as process_time,
            1,
            %(mid_sql)s
                on a.process_time = b.imp_date and a.advertiser_id = b.account_id
                WHERE
                    ((b.industry_group in ('金融')
                            and a.partition_time > date_sub(%(argvDate)s, 1 + 2)
                            and a.process_time = date_sub(%(argvDate)s, 1 + 1)
                      ) or (b.industry_group in ('教育', '家居', '房产', '大交通', '医药', '运营商','商务服务','招商加盟','旅游','本地生活')
                            and a.partition_time > date_sub(%(argvDate)s, 1 + 1)
                            and a.process_time = date_sub(%(argvDate)s, 1)))
                    and a.partition_time <= %(argvDate)s
                    and second_optimization_goal != 106
                    and (optimization_goal > 0 and optimization_goal != 7)
                    and a.site_set %(mpStr)s 21
                    and (a.no_compensation_type = 0 or a.no_compensation_type is null)
                    and is_rta_dpa_ad = false
                    and is_ocpx = 1
                    and auto_acquisition_status = 0
                GROUP BY adgroup_id, industry_group, second_optimization_goal) ad_base_info
            ) ad_reach_info
        GROUP BY op_industry
    """ % {"tableName": tableName, "argvDate": argvDate, "mid_sql": SQL_MID_TEMPLATE, "mpStr": mp_str}

    tdw.WriteLog('SQL:' + SQL)
    tdw.execute(SQL)

    SQL = """
INSERT
    TABLE %(tableName)s
        SELECT
            %(argvDate)s,
            %(argvDate)s as process_time,
            2,
            %(mid_sql)s
                on a.partition_time = b.imp_date and a.advertiser_id = b.account_id
                  and a.partition_time = %(argvDate)s
                WHERE b.industry_group in ('金融', '教育', '家居', '房产', '大交通', '医药', '运营商','商务服务','招商加盟','旅游','本地生活')
                    and a.site_set %(mpStr)s 21
                    and (not (a.no_compensation_type != 0 and a.no_compensation_type is not null))
                    and second_optimization_goal != 106
                    and (optimization_goal > 0 and optimization_goal != 7)
                    and is_rta_dpa_ad = false
                    and is_ocpx = true
                    and auto_acquisition_status = 0
                GROUP BY adgroup_id, industry_group, second_optimization_goal) ad_base_info
            ) ad_reach_info
        GROUP BY op_industry
    """ % {"tableName": tableName, "argvDate": argvDate, "mid_sql": SQL_MID_TEMPLATE, "mpStr": mp_str}

    tdw.WriteLog('SQL:' + SQL)
    tdw.execute(SQL)
