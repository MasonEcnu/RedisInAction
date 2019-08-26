#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json

from redis import Redis


# 查找IP所属城市以及国家


def ip_to_score(ip_address: str):
    score = 0
    if "/" in ip_address:
        ip_address = ip_address.split("/")[0]
    for v in ip_address.split("."):
        score = score * 256 + int(v, 10)
    return score


def import_ips_to_redis(conn: Redis, filename: str):
    # GeoLite2-City-Blocks-IPv4.csv
    csv_file = csv.reader(open(filename, "r"))
    for count, row in enumerate(csv_file):
        # 将Ip地址转为分值
        start_ip = row[0] if row else ""
        # 略过文件的列名行和格式错误行
        if "i" in start_ip.lower():
            continue
        if "." in start_ip:
            start_ip = ip_to_score(start_ip)
        elif start_ip.isdigit():
            start_ip = int(start_ip, 10)
        else:
            continue

        # 构建唯一城市Id
        city_id = row[2] + "_" + str(count)

        # 将城市Id极其Ip对应的分值存到redis中
        conn.zadd("ip2city_id:", {city_id: start_ip})


def import_cities_to_redis(conn: Redis, filename: str):
    # GeoLite2-City-Locations-*.csv
    csv_file = csv.reader(open(filename, "r", errors="ignore"))
    try:
        for row in csv_file:
            if len(row) < 4 or not row[0].isdigit():
                continue

            # row = [i.decode("latin-1") for i in row]
            city_id = row[0]
            country = row[1]
            region = row[2]
            city = row[3]

            # 将城市信息添加到redis中
            conn.hset("city_id2city:", city_id, json.dumps([city, region, country]))
    except UnicodeDecodeError as error:
        print(error)
        pass


ip_file = r"D:\PyCode\RedisInAction\location_file\GeoLite2-City-CSV_20190820\GeoLite2-City-Blocks-IPv4.csv"
city_file = r"D:\PyCode\RedisInAction\location_file\GeoLite2-City-CSV_20190820\GeoLite2-City-Locations-zh-CN.csv"

# import_ips_to_redis(redisClient, ip_file)
# import_cities_to_redis(redisClient, city_file)
