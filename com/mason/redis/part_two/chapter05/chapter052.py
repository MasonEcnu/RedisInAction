#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import bisect
import time

import redis
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


# 在处理（process）和清理（clean up）旧计数器的时候，有几件事情是需要我们格外留心的，
# 其中包括以下几件。
# 1.任何时候都可能会有新的计数器被添加进来。
# 2.同一时间可能会有多个不同的清理操作在执行。
# 3.对于一个每天只更新一次的计数器来说，以每分钟一次的频率尝试清理这个计数器只会
# 浪费计算资源。
# 4.如果一个计数器不包含任何数据，那么程序就不应该尝试对它进行清理。


QUIT_FLAG = False
SAMPLE_COUNT = 100


def clean_counters(conn: Redis):
    pipe = conn.pipeline(True)
    passes = 0
    while not QUIT_FLAG:
        start = time.time()
        index = 0
        while index < conn.zcard("known:"):
            hash_key = conn.zrange("known:", index, index)
            index += 1
            if not hash_key:
                break
            hash_key = hash_key[0]
            prec = int(hash_key.partition(":")[0])
            bprec = int(prec // 60) or 1
            if passes % bprec:
                continue

            hkey = "count:" + hash_key
            cutoff = time.time() - SAMPLE_COUNT * prec
            samples = list(map(int, conn.hkeys(hkey)))
            samples.sort()

            remove = bisect.bisect_right(samples, cutoff)
            if remove:
                conn.hdel(hkey, *samples[:remove])
                if remove == len(samples):
                    try:
                        pipe.watch(hkey)
                        if not pipe.hlen(hkey):
                            pipe.multi()
                            pipe.zrem("known:", hash_key)
                            pipe.execute()
                            index -= 1
                        else:
                            pipe.unwatch()
                    except redis.exceptions.WatchError:
                        pass

        passes += 1
        duration = min(int(time.time() - start) + 1, 60)
        time.sleep(max(60 - duration, 1))
