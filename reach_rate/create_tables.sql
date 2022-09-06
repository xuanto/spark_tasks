CREATE TABLE `achieve_rate_multi_trace_d` (
`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'ID',
`partition_date` bigint(20) DEFAULT NULL COMMENT "日期",
`process_date` bigint(20) DEFAULT NULL COMMENT "数据日期",
`table_id` bigint(20) DEFAULT NULL COMMENT "优化目标阶段",
`op_industry` Text DEFAULT NULL COMMENT "行业",
`ad_num` bigint(20) DEFAULT NULL COMMENT "广告总数",
`achieve_rate` float DEFAULT NULL COMMENT "广告达成率",
`below_rate` float DEFAULT NULL COMMENT "广告不超成本率",
`ec_rate_0` float DEFAULT NULL COMMENT "广告空耗率(完全没转化)",
`ec_rate_2` float DEFAULT NULL COMMENT "广告空耗率(转化量小于等于2)",
`cost` float DEFAULT NULL COMMENT "按照行业聚合总消耗",
`GMV` float DEFAULT NULL COMMENT "按照行业聚合总GMV",
`click_num` bigint(20) DEFAULT NULL COMMENT "广告总点击量",
`conversion_num` bigint(20) DEFAULT NULL COMMENT "广告总转化量",
`pcvr` float DEFAULT NULL COMMENT "预估转化率",
`cvr` float DEFAULT NULL COMMENT "实际转化率",
`pcvr_bias` float DEFAULT NULL COMMENT "转化率预估偏差",
`pctr` float DEFAULT NULL COMMENT "预估点击率",
`ctr` float DEFAULT NULL COMMENT "实际点击率",
`pctr_bias` float DEFAULT NULL COMMENT "点击率预估偏差",
`gsp_factor` float DEFAULT NULL COMMENT "计费比",
`real_gsp_factor` float DEFAULT NULL COMMENT "实际计费比",
PRIMARY KEY (`id`)
) AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='多口径达成率天级报表';

CREATE TABLE `finance_edu_action_bucket_reach_rate_analysis` (
`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'ID',
`partition_date` bigint(20) DEFAULT NULL COMMENT "日期",
`op_industry` Text DEFAULT NULL COMMENT "行业",
`op_industry2` Text DEFAULT NULL COMMENT "子行业",
`active_bucket` Text DEFAULT NULL COMMENT "转化数分桶",
`ad_num` bigint(20) DEFAULT NULL COMMENT "广告总数",
`achieve_rate` float DEFAULT NULL COMMENT "广告达成率",
`below_rate` float DEFAULT NULL COMMENT "广告不超成本率",
`ec_rate_0` float DEFAULT NULL COMMENT "广告空耗率(完全没转化)",
`ec_rate_2` float DEFAULT NULL COMMENT "广告空耗率(转化量小于等于2)",
`cost` float DEFAULT NULL COMMENT "按照行业聚合总消耗",
`GMV` float DEFAULT NULL COMMENT "按照行业聚合总GMV",
`click_num` bigint(20) DEFAULT NULL COMMENT "广告总点击量",
`conversion_num` bigint(20) DEFAULT NULL COMMENT "广告总转化量",
`pcvr` float DEFAULT NULL COMMENT "预估转化率",
`cvr` float DEFAULT NULL COMMENT "实际转化率",
`pcvr_bias` float DEFAULT NULL COMMENT "转化率预估偏差",
`pctr` float DEFAULT NULL COMMENT "预估点击率",
`ctr` float DEFAULT NULL COMMENT "实际点击率",
`pctr_bias` float DEFAULT NULL COMMENT "点击率预估偏差",
`gsp_factor` float DEFAULT NULL COMMENT "计费比",
`real_gsp_factor` float DEFAULT NULL COMMENT "实际计费比",
PRIMARY KEY (`id`)
) AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='金融教育子行业转化数分桶达成率报表';

CREATE TABLE `industry_conv_bucket_new_reach_rate_d` (
`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'ID',
`partition_date` bigint(20) DEFAULT NULL COMMENT "日期",
`op_industry` Text DEFAULT NULL COMMENT "行业",
`active_bucket` Text DEFAULT NULL COMMENT "转化数分桶",
`ad_num` bigint(20) DEFAULT NULL COMMENT "广告总数",
`achieve_rate` float DEFAULT NULL COMMENT "综合达成率",
`cost` float DEFAULT NULL COMMENT "按照行业聚合总消耗",
`GMV` float DEFAULT NULL COMMENT "按照行业聚合总GMV",
`click_num` bigint(20) DEFAULT NULL COMMENT "广告总点击量",
`conversion_num` bigint(20) DEFAULT NULL COMMENT "广告总转化量",
`pcvr` float DEFAULT NULL COMMENT "预估转化率",
`cvr` float DEFAULT NULL COMMENT "实际转化率",
`pcvr_bias` float DEFAULT NULL COMMENT "转化率预估偏差",
`pctr` float DEFAULT NULL COMMENT "预估点击率",
`ctr` float DEFAULT NULL COMMENT "实际点击率",
`pctr_bias` float DEFAULT NULL COMMENT "点击率预估偏差",
PRIMARY KEY (`id`)
) AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='转化数分桶综合达成率报表';

ALTER TABLE industry_conv_bucket_new_reach_rate_d
ADD COLUMN ad_reach_rate float DEFAULT NULL COMMENT "广告达成率" AFTER achieve_rate,
ADD COLUMN ac_reach_rate float DEFAULT NULL COMMENT "账户达成率" AFTER ad_reach_rate;


CREATE TABLE `new_reach_rate_report_d` (
`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'ID',
`partition_date` bigint(20) DEFAULT NULL COMMENT "日期",
`process_date` bigint(20) DEFAULT NULL COMMENT "数据日期",
`op_industry` Text DEFAULT NULL COMMENT "行业",
`ad_num` bigint(20) DEFAULT NULL COMMENT "广告总数",
`achieve_rate` float DEFAULT NULL COMMENT "综合达成率",
`ad_reach_rate` float DEFAULT NULL COMMENT "广告达成率",
`ac_reach_rate` float DEFAULT NULL COMMENT "账户达成率",
`cost` float DEFAULT NULL COMMENT "按照行业聚合总消耗",
`GMV` float DEFAULT NULL COMMENT "按照行业聚合总GMV",
`click_num` bigint(20) DEFAULT NULL COMMENT "广告总点击量",
`conversion_num` bigint(20) DEFAULT NULL COMMENT "广告总转化量",
`pcvr` float DEFAULT NULL COMMENT "预估转化率",
`cvr` float DEFAULT NULL COMMENT "实际转化率",
`pcvr_bias` float DEFAULT NULL COMMENT "转化率预估偏差",
`pctr` float DEFAULT NULL COMMENT "预估点击率",
`ctr` float DEFAULT NULL COMMENT "实际点击率",
`pctr_bias` float DEFAULT NULL COMMENT "点击率预估偏差",
PRIMARY KEY (`id`)
) AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='综合口径达成率天级报表';

CREATE TABLE `vip_sub_brand_new_reach_rate_d` (
`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'ID',
`partition_date` bigint(20) DEFAULT NULL COMMENT "日期",
`op_industry2` Text DEFAULT NULL COMMENT "ops二级行业",
`sub_brand` Text DEFAULT NULL COMMENT "子品牌",
`ad_num` bigint(20) DEFAULT NULL COMMENT "广告总数",
`achieve_rate` float DEFAULT NULL COMMENT "综合达成率",
`ad_reach_rate` float DEFAULT NULL COMMENT "新广告达成率",
`ac_reach_rate` float DEFAULT NULL COMMENT "账户达成率",
`reach_rate` float DEFAULT NULL COMMENT "广告达成率",
`below_rate` float DEFAULT NULL COMMENT "不超成本达成率",
`cost` float DEFAULT NULL COMMENT "按照行业聚合总消耗",
`gmv` float DEFAULT NULL COMMENT "按照行业聚合总GMV",
`click_num` bigint(20) DEFAULT NULL COMMENT "广告总点击量",
`conversion_num` bigint(20) DEFAULT NULL COMMENT "广告总转化量",
`pcvr` float DEFAULT NULL COMMENT "预估转化率",
`cvr` float DEFAULT NULL COMMENT "实际转化率",
`pcvr_bias` float DEFAULT NULL COMMENT "转化率预估偏差",
`pctr` float DEFAULT NULL COMMENT "预估点击率",
`ctr` float DEFAULT NULL COMMENT "实际点击率",
`pctr_bias` float DEFAULT NULL COMMENT "点击率预估偏差",
PRIMARY KEY (`id`)
) AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='重点子品牌综合达成率报表';

CREATE TABLE `vip_sub_brand_bucket_reach_rate_d` (
`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'ID',
`partition_date` bigint(20) DEFAULT NULL COMMENT "日期",
`op_industry2` Text DEFAULT NULL COMMENT "ops二级行业",
`sub_brand` Text DEFAULT NULL COMMENT "子品牌",
`active_bucket` Text DEFAULT NULL COMMENT "转化数分桶",
`ad_num` bigint(20) DEFAULT NULL COMMENT "广告总数",
`achieve_rate` float DEFAULT NULL COMMENT "综合达成率",
`ad_reach_rate` float DEFAULT NULL COMMENT "新广告达成率",
`ac_reach_rate` float DEFAULT NULL COMMENT "账户达成率",
`reach_rate` float DEFAULT NULL COMMENT "广告达成率",
`below_rate` float DEFAULT NULL COMMENT "不超成本达成率",
`cost` float DEFAULT NULL COMMENT "按照行业聚合总消耗",
`gmv` float DEFAULT NULL COMMENT "按照行业聚合总GMV",
`click_num` bigint(20) DEFAULT NULL COMMENT "广告总点击量",
`conversion_num` bigint(20) DEFAULT NULL COMMENT "广告总转化量",
`pcvr` float DEFAULT NULL COMMENT "预估转化率",
`cvr` float DEFAULT NULL COMMENT "实际转化率",
`pcvr_bias` float DEFAULT NULL COMMENT "转化率预估偏差",
`pctr` float DEFAULT NULL COMMENT "预估点击率",
`ctr` float DEFAULT NULL COMMENT "实际点击率",
`pctr_bias` float DEFAULT NULL COMMENT "点击率预估偏差",
`gmv_to_cost` float DEFAULT NULL COMMENT "gmv比cost",
PRIMARY KEY (`id`)
) AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='子品牌转化分桶达成率报表';

CREATE TABLE `medicine_sub_industry_reach_rate_d` (
`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'ID',
`partition_date` bigint(20) DEFAULT NULL COMMENT "日期",
`op_industry2` Text DEFAULT NULL COMMENT "ops二级行业",
`ad_num` bigint(20) DEFAULT NULL COMMENT "广告总数",
`achieve_rate` float DEFAULT NULL COMMENT "广告达成率",
`below_rate` float DEFAULT NULL COMMENT "广告不超成本率",
`ec_rate_0` float DEFAULT NULL COMMENT "广告空耗率(完全没转化)",
`ec_rate_2` float DEFAULT NULL COMMENT "广告空耗率(转化量小于等于2)",
`cost` float DEFAULT NULL COMMENT "按照行业聚合总消耗",
`GMV` float DEFAULT NULL COMMENT "按照行业聚合总GMV",
`click_num` bigint(20) DEFAULT NULL COMMENT "广告总点击量",
`conversion_num` bigint(20) DEFAULT NULL COMMENT "广告总转化量",
`pcvr` float DEFAULT NULL COMMENT "预估转化率",
`cvr` float DEFAULT NULL COMMENT "实际转化率",
`pcvr_bias` float DEFAULT NULL COMMENT "转化率预估偏差",
`pctr` float DEFAULT NULL COMMENT "预估点击率",
`ctr` float DEFAULT NULL COMMENT "实际点击率",
`pctr_bias` float DEFAULT NULL COMMENT "点击率预估偏差",
`gsp_factor` float DEFAULT NULL COMMENT "计费比",
`real_gsp_factor` float DEFAULT NULL COMMENT "实际计费比",
PRIMARY KEY (`id`)
) AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='医药子行业达成率报表';

CREATE TABLE `medicine_sub_industry_conv_bucket_reach_rate_d` (
`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'ID',
`partition_date` bigint(20) DEFAULT NULL COMMENT "日期",
`op_industry` Text DEFAULT NULL COMMENT "行业",
`op_industry2` Text DEFAULT NULL COMMENT "子行业",
`active_bucket` Text DEFAULT NULL COMMENT "转化数分桶",
`ad_num` bigint(20) DEFAULT NULL COMMENT "广告总数",
`achieve_rate` float DEFAULT NULL COMMENT "广告达成率",
`below_rate` float DEFAULT NULL COMMENT "广告不超成本率",
`ec_rate_0` float DEFAULT NULL COMMENT "广告空耗率(完全没转化)",
`ec_rate_2` float DEFAULT NULL COMMENT "广告空耗率(转化量小于等于2)",
`cost` float DEFAULT NULL COMMENT "按照行业聚合总消耗",
`GMV` float DEFAULT NULL COMMENT "按照行业聚合总GMV",
`click_num` bigint(20) DEFAULT NULL COMMENT "广告总点击量",
`conversion_num` bigint(20) DEFAULT NULL COMMENT "广告总转化量",
`pcvr` float DEFAULT NULL COMMENT "预估转化率",
`cvr` float DEFAULT NULL COMMENT "实际转化率",
`pcvr_bias` float DEFAULT NULL COMMENT "转化率预估偏差",
`pctr` float DEFAULT NULL COMMENT "预估点击率",
`ctr` float DEFAULT NULL COMMENT "实际点击率",
`pctr_bias` float DEFAULT NULL COMMENT "点击率预估偏差",
`gsp_factor` float DEFAULT NULL COMMENT "计费比",
`real_gsp_factor` float DEFAULT NULL COMMENT "实际计费比",
PRIMARY KEY (`id`)
) AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='医药子行业转化分桶达成率报表';