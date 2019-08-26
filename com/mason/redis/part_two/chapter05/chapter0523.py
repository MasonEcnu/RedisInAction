#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import contextlib
import time
from urllib.request import Request

from redis import Redis

from com.mason.redis.part_two.chapter05.chapter0522 import update_status


# 使用 Redis 存储统计数据

# 将这个Python生成器用作上下文管理器
@contextlib.contextmanager
def access_time(conn: Redis, context):
    # 记录代码块执行前的时间
    start = time.time()
    # 运行被包裹的代码块
    # yield 的作用就是把一个函数变成一个 generator
    yield
    # 计算执行时长
    delta = time.time() - start
    # 更新统计数据
    status = update_status(conn, context, "AccessTime", delta)
    # 计算平均时间
    average = status[1] / status[0]

    pipe = conn.pipeline(True)
    # 将页面的平均访问时长添加到记录最长访问时间的有序集合里
    pipe.zadd("slowest:AccessTime", context, average)
    # AccessTime有序集合只会保留最难的100条记录
    pipe.zremrangebyrank("slowest:AccessTime", 0, -101)
    pipe.execute()


def process_view(conn: Redis, request: Request, callback):
    # 这个request是哪定义的啊。。。
    # 计算并记录访问时长的上下文管理器就是这样包围代码块的。
    with access_time(conn, request.full_url()):
        # 当上下文管理器中的 yield语句被执行时，这个语句就会被执行。
        return callback()
