# -*- coding:utf-8 -*-
"""
Author: Ning Wang <█████@█████.com>
转换crm行业id为1级和2级行业id，在本项目中未使用
"""


def level_one_industry_id(adcategoryid):
    if adcategoryid - 21474836480 < 100:
        return adcategoryid - 21474836480
    else:
        return (adcategoryid - 21474836480) / 100


def level_two_industry_id(adcategoryid):
    return adcategoryid - 21474836480
