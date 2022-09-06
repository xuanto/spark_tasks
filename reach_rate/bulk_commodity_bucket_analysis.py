# coding: utf-8
"""
Copyright (c) 2021, Tencent Inc.
All Rights Reserved.
Author: (╯°□°）╯︵┻━┻) <(╯°□°）╯︵┻━┻)@tencent.com>
车房医家，按照转化数和pcvr_bias分桶统计其达成率，不超成本率等信息

sql部分内容可以直接跑
"""

from datetime import datetime, timedelta


def TDW_PL(tdw, argv):
    SQL = """
    SELECT
        op_industry,
        op_industry2,
        active_bucket,
        og,
        bias_bucket,
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
            op_industry,
            op_industry2,
            active_bucket,
            if(second_goal = 0, smart_optimization_goal, second_goal) as og,
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
            real_gsp_factor,
            CASE
              WHEN (
                (second_goal = 0 and conversion_num = 0) or (second_goal > 0 and second_conversion_num = 0)
                ) THEN '偏差大于20'
              WHEN (
                (second_goal = 0 and ABS((valid_click_pcvr_sum / conversion_num) - 1) > 0.2)
                    or (second_goal > 0 and ABS((valid_click_pcvr2_sum / second_conversion_num) - 1) > 0.2)
                ) THEN '偏差大于20'
              WHEN (
                (second_goal = 0 and ABS((valid_click_pcvr_sum / conversion_num) - 1) > 0.1)
                    or (second_goal > 0 and ABS((valid_click_pcvr2_sum / second_conversion_num) - 1) > 0.1)
                ) THEN '偏差大于10小于等于20'
              ELSE '偏差小于等于10'
            END as bias_bucket
        FROM (
            SELECT
                adgroup_id,
                b.industry_group as op_industry,
                b.industry_team_level2 as op_industry2,
                smart_optimization_goal,
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
                SUM(valid_gsp_exposure_gsp_factor) / SUM(valid_gsp_exposure_cnt) as real_gsp_factor,
                CASE
                    WHEN (
                        (second_goal = 0 and (SUM(active_num) > 30 or SUM(real_cost_micros) / 10000 > 30 * SUM(valid_click_target_cpa) / SUM(valid_click_cnt)))
                            or (second_goal > 0 and (SUM(second_active_num) > 30 or SUM(real_cost_micros) / 10000 > 30 * SUM(vc_second_bid) / SUM(valid_click_cnt)))
                    ) THEN '转化大于30'
                    WHEN (
                        (second_goal = 0 and (SUM(active_num) > 6 or SUM(real_cost_micros) / 10000 > 6 * SUM(valid_click_target_cpa) / SUM(valid_click_cnt)))
                            or (second_goal > 0 and (SUM(second_active_num) > 6 or SUM(real_cost_micros) / 10000 > 6 * SUM(vc_second_bid) / SUM(valid_click_cnt)))
                    ) THEN '转化大于6小于等于30'
                    ELSE '转化小于等于6'
                END as active_bucket
            FROM hlw_gdt::t_ocpa_middle_table_d a
            join ams_access_db::t_ad_accounts_full_d b
            on a.partition_date = b.imp_date and a.advertiser_id = b.account_id
              and a.site_set != 21
            WHERE b.industry_group in ('家居', '房产', '大交通', '医药') and a.partition_date >= 20210904
                  and a.partition_date <= 20210910
            GROUP BY adgroup_id, b.industry_group, b.industry_team_level2, smart_optimization_goal, second_goal) ad_base_info
        WHERE real_cost_sum > 0
        ) ad_reach_info
    GROUP BY op_industry, op_industry2, active_bucket, og, bias_bucket"""

    tdw.WriteLog('SQL:' + SQL)
    tdw.execute(SQL)
