#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time

from redis import Redis

# 计数器和数据统计
# 时间序列计数器（time series counter）

# 计数器精度
# 单位：秒
PRECISION = [1, 5, 60, 300, 3600, 18000, 86400]


def update_counter(conn: Redis, name, count=1, now=None):
    # 通过取的当前时间，以判断需要对哪个时间片执行自增操作
    now = now or time.time()
    # 为了保证后续的清理工作可以正确的执行
    # 这里需要用事务型流水线
    pipe = conn.pipeline()
    # 为每一个精度创建一个计数器
    for prec in PRECISION:
        p_now = int(now / prec) * prec
        hash_key = "%s:%s" % (prec, name)
        # 将计数器的引用信息添加到有序集合，并将分值设置为0，以便后期执行清理
        pipe.zadd("known:", hash_key, 0)
        # 对给定名字的计数器进行自增
        pipe.hincrby("count:" + hash_key, p_now, count)
    pipe.execute()


def get_counter(conn: Redis, name, precision):
    hash_key = "%s:%s" % (precision, name)
    data = conn.hgetall("count:" + hash_key)
    to_return = []
    # key:时间片，value:点击次数
    for key, value in data.iteritems():
        to_return.append((int(key), int(value)))
    # 按key升序，key相同时，按value升序
    to_return.sort()
    return to_return
