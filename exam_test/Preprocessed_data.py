# -*- coding: utf-8 -*-
import csv
import string
import pandas

from random import choice
province = ["北京", "上海", "天津", "重庆", "河北", "山西", "内蒙古", "辽宁", "吉林", "黑龙江", "江苏", "浙江", "安徽", "福建", "江西", "山东",
            "河南", "湖北", "湖南", "广东", "广西", "海南", "四川", "陕西", "甘肃", "贵州", "云南", "西藏", "宁夏", "青海", "新疆", "香港", "澳门", "台湾"]


def getTimedata(path, mode):
    with open(path, mode) as Data:
        time = []
        datas = csv.reader(Data)
        for line in datas:
            line[-1] = str(line[-1]).rstrip(string.digits).strip()
            time.append(line[-1])
        return time[1::]


provinces = []
for i in range(300054):
    prov = choice(province)
    provinces.append(prov)


df = pandas.read_csv("sale_user.csv", encoding='utf-8', low_memory=False)
df['province'] = pandas.Series(provinces)
times = getTimedata('sale_user.csv', 'r')
df['time'] = pandas.Series(times)
df.to_csv('final_user_test.csv', columns=['user_id', 'item_id', 'behavior_type',
                                     'item_category', 'time', 'province'], index=False, header=1)
