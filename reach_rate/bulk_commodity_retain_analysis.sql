-- 车房家医次留广告统计

SELECT
    process_date,
    advertiser_industry,
    SUM(sum_cost_v0) + SUM(sum_cost_v1) as total_cost,
    SUM(sum_cost_v0) as sum_cost_v0,
    SUM(sum_cost_v1) as sum_cost_v1,
    SUM(exposure_cnt) as exposure_cnt,
    SUM(click_cnt) as click_cnt,
    SUM(active_num) as active_num,
    SUM(first_day_active_num) as first_day_active_num,
    SUM(retain_num) as retain_num,
    SUM(pdcvr_retain_num) as pdcvr_retain_num,
    SUM(first_day_pdcvr_retain_num) as first_day_pdcvr_retain_num,
    SUM(below_rate_target * sum_cost_v0) as below_rate_cost,
    SUM(below_rate_target * sum_cost_v0) / SUM(sum_cost_v0) as below_rate,
    SUM(dcvr_reached * below_rate_target * sum_cost_v0) as dcvr_reached_cost,
    SUM(dcvr_reached * below_rate_target * sum_cost_v0) / SUM(sum_cost_v0) as dcvr_reach_rate
FROM
    (SELECT
        process_date,
        adgroup_id,
        b.industry_group as advertiser_industry,
        b.industry_team_level2 as op_industry2,
        advertiser_id,
        smart_optimization_goal as first_goal,
        second_goal,
        bid_strategy,
        auto_acquisition_status,
        MAX(if(bid_strategy = 2, 1, 0)) as use_amount_first,
        (SUM(real_cost_micros) / 10000 / SUM(active_num)) / (SUM(valid_click_target_cpa) / SUM(valid_click_cnt)) - 1 AS first_cpa_bias,
        SUM(if((auto_acquisition_status!=1), real_cost_micros/1000000, 0)) as sum_cost_v0,
        SUM(if((auto_acquisition_status==1), real_cost_micros/1000000, 0)) as sum_cost_v1,
        SUM(valid_exposure_cnt) as exposure_cnt,
        SUM(valid_click_cnt) as click_cnt,
        SUM(active_num) as active_num,
        SUM(if(partition_date == process_date, active_num, 0)) as first_day_active_num,
        SUM(second_active_num) as retain_num,
        SUM(real_cost_micros) / (1000000*sum(active_num)) as cpa1,
        SUM(target_cpa_v2)/ (sum(valid_click_cnt) + sum(valid_exposure_cnt))/100 as target_cpa1,
        SUM(real_cost_micros) / (1000000*sum(if(action_type==106, trace_cnt, 0))) as cpa2,
        SUM(ocpa_second_target_cpa)/ (100* sum (if(bid_type==1, valid_click_cnt, valid_exposure_cnt))) as valid_tcpa2,
        SUM(valid_click_smart_pcvr)/1000000 as pcvr1_sum,
        SUM(vc_ltv_info_smart_pcvr2) as pcvr2_sum,
        SUM(if((action_type==104 or action_type==105), vt_smart_deep_pcvr, 0))/1000000 as pdcvr_retain_num,
        SUM(if(((action_type == 104 or action_type == 105) and partition_date == process_date), vt_smart_deep_pcvr, 0))/1000000 as first_day_pdcvr_retain_num,
        if((SUM(real_cost_micros) / 10000 / SUM(active_num)) / (SUM(valid_click_target_cpa) / SUM(valid_click_cnt)) - 1 <= (0.2 + 0.1 * MAX(if(bid_strategy = 2, 1, 0))), 1, 0) as below_rate_target,
        if((SUM(second_active_num) / SUM(if(((action_type == 104 or action_type == 105) and partition_date == process_date), vt_smart_deep_pcvr, 0))/1000000) >= (SUM(target_cpa_v2)/ (sum(valid_click_cnt) + sum(valid_exposure_cnt))/100 / SUM(ocpa_second_target_cpa)/ (100* sum (if(bid_type==1, valid_click_cnt, valid_exposure_cnt)))), 1, 0) as dcvr_reached,
        SUM(active_num) * sum(vc_ltv_info_smart_pcvr2)*1000000/sum(valid_click_smart_pcvr) as div_pcvr_retain_num
    FROM hlw_gdt::t_ocpa_middle_table_d a
    JOIN ams_access_db::t_ad_accounts_full_d b
    ON a.partition_date = b.imp_date and a.advertiser_id = b.account_id
        and a.site_set != 21
        and (a.no_compensation_type = 0 or a.no_compensation_type is null)
    WHERE second_goal = 106
        and process_date = 20210909
        and partition_date <= 20210910
        and partition_date >= 20210909
        and b.industry_group in ('家居', '房产', '大交通', '医药')
    group by process_date,
        adgroup_id,
        b.industry_group,
        b.industry_team_level2,
        advertiser_id,
        smart_optimization_goal,
        second_goal,
        bid_strategy,
        auto_acquisition_status
    )
group by process_date, advertiser_industry