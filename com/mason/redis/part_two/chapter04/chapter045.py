#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time

from redis import Redis


# 非事务型流水线（non-transactional pipeline）
# 被 MULTI 和 EXEC 包裹的命令在执行时不会被其他客户端打扰

# 改进
def update_token_pipeline(conn: Redis, token, user, item=None):
    timestamp = time.time()
    # 设置流水线
    pipe = conn.pipeline(False)
    pipe.hset("login:", token, user)
    pipe.zadd("recent:", {token: timestamp})
    if item:
        pipe.zadd("viewed:" + token, {item: timestamp})
        pipe.zremrangebyrank("viewed:" + token, 0, -26)
        pipe.zincrby("viewed:", item, -1)
        # 执行流水线包裹的任务
        pipe.execute()


def update_token(conn, token, user, item=None):
    timestamp = time.time()
    conn.hset("login:", token, user)
    conn.zadd("recent:", {token: timestamp})
    if item:
        conn.zadd("viewed:" + token, {item: timestamp})
        conn.zremrangebyrank("viewed:" + token, 0, -26)
        conn.zincrby("viewed:", item, -1)


def benchmark_update_token(conn: Redis, duration):
    for function in (update_token, update_token_pipeline):
        count = 0
        start = time.time()
        end = start + duration
        while time.time() < end:
            count += 1
            function(conn, "token", "user", "item")
        delta = time.time() - start
        print(function.__name__, count, delta, count / delta)

# 以 Python 的 Redis
# 客户端为例，对于每个 Redis 服务器，用户只需要创建一个 redis.Redis()对象，该对象就会
# 按需创建连接、重用已有的连接并关闭超时的连接（在使用多个数据库的情况下，即使客户端只
# 连接了一个 Redis 服务器，它也需要为每一个被使用的数据库创建一个连接），并且 Python 客户
# 端的连接池还可以安全地应用于多线程环境和多进程环境。
