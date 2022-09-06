SELECT
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
            partition_date,
            adgroup_id,
            bias_bucket,
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
                a.partition_date as partition_date,
                adgroup_id,
                second_goal,
                MAX(if(bid_strategy = 2, 1, 0)) as use_amount_first,
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
                        SUM(second_active_num) = 0
                        ) THEN '偏差大于20'
                    WHEN (
                        ABS((SUM(vc_adjusted_smart_pcvr2) / 1000000 / SUM(second_active_num)) - 1) > 0.2
                        ) THEN '偏差大于20'
                    WHEN (
                        ABS((SUM(vc_adjusted_smart_pcvr2) / 1000000 / SUM(second_active_num)) - 1) > 0.1
                        ) THEN '偏差大于10小于等于20'
                    ELSE '偏差小于等于10'
                END as bias_bucket
            FROM hlw_gdt::t_ocpa_middle_table_d a
            join ams_access_db::t_ad_accounts_full_d b
            on a.partition_date = b.imp_date and a.advertiser_id = b.account_id
              and a.site_set != 21
            WHERE a.partition_date >= 20211025
                  and a.partition_date <= 20211031
                --   and b.industry_group in ('房产')
                  and a.smart_optimization_goal = 104
                  and a.second_goal = 106
            GROUP BY a.partition_date, adgroup_id, second_goal
          ) ad_base_info
        ) ad_reach_info
GROUP BY bias_bucket
