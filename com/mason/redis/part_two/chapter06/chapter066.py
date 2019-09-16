#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import csv
from collections import defaultdict

# 文件分发
# 一个本地聚合计算回调函数
# 用于每天以国家维度对日志行进行聚合
from redis import Redis

# 准备本地聚合数据字典
aggregates = defaultdict(lambda: defaultdict(int))


def daily_country_aggregate(conn: Redis, line: str):
    if line:
        # 默认按空格分割
        # 提取日志行中的信息
        line = line.split()
        ip = line[0]
        day = line[1]
        # 根据Ip地址判断用户所在国家
        country = find_city_by_ip_local(ip)[2]
        if not country:
            country = -1
        # 对本地聚合数据执行自增
        aggregates[day][country] += 1
        return

    # 当天日志处理完毕
    # 将聚合计算的结果写入Redis里
    for day, aggregate in aggregates.items():
        conn.zadd("daily:country:" + day, **aggregate)
        del aggregates[day]


def find_city_by_ip_local(ip: str):
    ip_file = r"D:\PyCode\RedisInAction\location_file\GeoLite2-City-CSV_20190820\GeoLite2-City-Blocks-IPv4.csv"
    csv_file = csv.reader(open(ip_file, "r"))
    for count, row in enumerate(csv_file):
        # 将Ip地址转为分值
        start_ip = row[0] if row else ""
        # 略过文件的列名行和格式错误行
        if "i" in start_ip.lower() or start_ip == "":
            continue
        if "/" in start_ip:
            start_ip = start_ip.split("/")[0]
        if "." in start_ip and start_ip.__eq__(ip):
            return row
        else:
            return None
