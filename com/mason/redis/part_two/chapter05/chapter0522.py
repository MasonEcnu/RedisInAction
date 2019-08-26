#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import uuid
from datetime import datetime

import redis
from redis import Redis

# 使用 Redis 存储统计数据
from com.mason.redis_client import redisClient


def update_status(conn: Redis, context, context_type, value, timeout=5):
    # 负责存储统计数据的键
    destination = "status:%s:%s" % (context, context_type)
    start_key = destination + ":start"
    pipe = conn.pipeline(True)
    end = time.time() + timeout
    # 处理当前这一个小时和上一个小时的数据
    while time.time() < end:
        try:
            pipe.watch(start_key)
            now = datetime.utcnow().timetuple()
            hour_start = datetime(*now[:4]).isoformat()

            existing = pipe.get(start_key)
            pipe.multi()
            if existing and existing < hour_start:
                pipe.rename(destination, destination + ":last")
                pipe.rename(start_key, destination + ":pstart")
                pipe.set(start_key, hour_start)

            tkey1 = str(uuid.uuid4())
            tkey2 = str(uuid.uuid4())

            # 将值添加到临时键里
            pipe.zadd(tkey1, {"min": value})
            pipe.zadd(tkey2, {"max": value})

            # 使用聚合函数MIN和MAX
            # 对存储统计数据的键
            # 以及两个临时键进行合并计算
            pipe.zunionstore(destination, [destination, tkey1], aggregate="min")
            pipe.zunionstore(destination, [destination, tkey2], aggregate="max")

            # 删除临时键
            pipe.delete(tkey1, tkey2)
            # 对有序集合中的样本数量、值的和、值的平方和进行更新
            pipe.zincrby(destination, 1, "count")
            pipe.zincrby(destination, value, "sum")
            pipe.zincrby(destination, value * value, "sumsq")

            # 返回基本的计数信息
            # 以便函数调用者在有需要时做进一步处理
            return pipe.execute()[-3:]
        except redis.exceptions.WatchError:
            # 如果新的一个小时已经开始
            # 并且旧的数据已经归档
            # 那么进行重试
            continue


context = "context"
context_type = "context_type"
destination = "status:%s:%s" % (context, context_type)

update_status(redisClient, context, context_type, 2)


def get_status(conn: Redis, context, context_type):
    destination = "status:%s:%s" % (context, context_type)
    data = dict(conn.zrange(destination, 0, -1, withscores=True))
    data["average"] = data["sum"] / data["count"]
    # 计算标准差
    numerator = data["sumsq"] - data["sum"] ** 2 / data["count"]
    data["stddev"] = (numerator / data["count"] - 1 or 1) ** 0.5
    return data


print(get_status(redisClient, context, context_type))

# 标准差公式
# 开方{ [平方和 - (和平方 / n)] / (n - 1) }
