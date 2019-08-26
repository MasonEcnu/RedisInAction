#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json

from redis import Redis

from com.mason.redis.part_two.chapter05.chapter053 import ip_to_score
# 查找 IP 所属城市
from com.mason.redis_client import redisClient


def find_city_by_ip(conn: Redis, ip_address):
    if isinstance(ip_address, str):
        ip_address = ip_to_score(ip_address)
    else:
        return None
    city_id = conn.zrevrangebyscore("ip2city_id:", ip_address, 0, start=0, num=1)
    if not city_id:
        return None
    city_id = city_id[0].partition("_")[0]
    return json.loads(conn.hget("city_id2city:", city_id))


ip = "61.174.15.215"
print(find_city_by_ip(redisClient, ip))
