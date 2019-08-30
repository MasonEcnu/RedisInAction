#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# 公平信号量
import time
import uuid

from redis import Redis


def acquire_fair_semaphore(conn: Redis, sem_name, limit, timeout=10):
    identifier = str(uuid.uuid4())
    c_zset = sem_name + ":owner"
    c_str = sem_name + ":counter"

    now = time.time()
    pipe = conn.pipeline(True)
    # 删除超时的信号量
    pipe.zremrangebyscore(sem_name, "-inf", now - timeout)
    pipe.zinterstore(c_zset, {c_zset: 1}, {sem_name: 0})

    # 对计数器执行自增操作
    pipe.incr(c_str)
    # 获取计数器自增之后的值
    counter = pipe.execute()[-1]

    # 通过检查排名来判断客户端是否获取了信号量
    pipe.zadd(sem_name, identifier, now)
    pipe.zadd(c_zset, identifier, counter)

    pipe.zrank(c_zset, identifier)
    if pipe.execute()[-1] < limit:
        return identifier

    # 客户端未能获取信号量，清理无用数据
    pipe.zrem(sem_name, identifier)
    pipe.zrem(c_zset, identifier)
    pipe.execute()
    return None


def release_fair_semaphore(conn: Redis, sem_name, identifier):
    pipe = conn.pipeline(True)
    pipe.zrem(sem_name, identifier)
    pipe.zrem(sem_name + ":owner", identifier)
    # 返回True表示信号量被正确的释放了
    # 返回False表示信号量已经超时自动删除了
    return pipe.execute()[0]
