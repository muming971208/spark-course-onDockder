# -*- coding: utf-8 -*-

import pyecharts
import os
import json
import traceback

import pymysql.cursors
import pyecharts.options as opts
from example.commons import Faker
from pyecharts.charts import Line
from pyecharts.charts import Pie


def connect_mysql():
    """ 创建链接 """
    try:
        with open("./db_config.json", "r") as file:
            load_dict = json.load(file)
        return pymysql.connect(cursorclass=pymysql.cursors.DictCursor, **load_dict)
    except Exception as e:
        print("cannot create mysql connect")


def removeProv(li, pro):
    pro.append(li[0])
    del li[0]
    return li


def line_base() -> Line:

    db = connect_mysql()
    cursor = db.cursor()
    sql = "select * from user_stat"
    cursor.execute(sql)
    row3 = cursor.fetchall()
    l = []
    province = []
    for item in row3:
        l.append(removeProv(list(item.values()), province))
    browse = [item[0] for item in l]
    favorite = [item[1] for item in l]
    buycar = [item[2] for item in l]
    buy = [item[3] for item in l]
    c = (
        Line()
        .add_xaxis(province)
        .add_yaxis("browse", browse, is_smooth=True)
        .add_yaxis("favorite", favorite, is_smooth=True)
        .add_yaxis("buycar", buycar, is_smooth=True)
        .add_yaxis("buy", buy, is_smooth=True)
        .set_global_opts(title_opts=opts.TitleOpts(title="各省市用户上网所有动态的总数"))
    )
    return c


def pie_base() -> Pie:
    db = connect_mysql()
    cursor = db.cursor()
    sql = "select * from user_stat"
    cursor.execute(sql)
    row3 = cursor.fetchall()
    l = []
    province = []
    for item in row3:
        l.append(removeProv(list(item.values()), province))
    browse = [item[0] for item in l]
    favorite = [item[1] for item in l]
    buycar = [item[2] for item in l]
    buy = [item[3] for item in l]
    c = (
        Pie()
        .add("", [list(z) for z in zip(province, browse)], center=["55%", "60%"])
        .set_global_opts(title_opts=opts.TitleOpts(title="各省市浏览总次数比重"),legend_opts=opts.LegendOpts(pos_left="25%"))
        .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}"))
    )
    return c


# line_base().render(path='line_page.html')
pie_base().render(path='pie_page.html')
