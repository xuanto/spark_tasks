-- idex删除分区
use ams_industry2;
ALTER TABLE achieve_rate_report_hist_data_d DROP PARTITION(p_20220119);
ALTER TABLE mp_reach_rate_report_hist_data_d DROP PARTITION(p_20220119);

-- reports 库 删除分区
delete from mp_new_reach_rate_report_d where partition_date=20220119;
delete from mp_reach_rate_multi_trace_d where partition_date=20220119;
delete from mp_industry_conv_bucket_new_reach_rate_d where partition_date=20220119;
delete from mp_vip_sub_brand_new_reach_rate_d where partition_date=20220119;



