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
    # 为了平等地处理更新频率各不相同的多个计数器
    # 程序需要记录清理操作的执行次数
    passes = 0
    # 持续地对计数器进行清理，直到退出为止
    while not QUIT_FLAG:
        # 记录清理操作开始执行的时间
        # 用于计算清理操作的执行时长
        start = time.time()
        index = 0
        # 渐进地遍历所有已知计数器
        while index < conn.zcard("known:"):
            # 取得被检查计数器的数据
            hash_key = conn.zrange("known:", index, index)
            index += 1
            if not hash_key:
                break
            hash_key = hash_key[0]
            # 取的计数器的精度
            prec = int(hash_key.partition(":")[0])
            # 因为程序每60秒就会循环一次
            # 所以这里需要根据计数器的更新频率
            # 判断是否有必要对计数器进行清理
            bprec = int(prec // 60) or 1
            # 如果这个计数器在这次循环里不需要进行清理，那么检查下一
            # 个计数器。（举个例子，如果清理程序只循环了3 次，而计数器
            # 的更新频率为每 5 分钟一次，那么程序暂时还不需要对这个计
            # 数器进行清理。）
            if passes % bprec:
                continue

            hkey = "count:" + hash_key
            # 根据给定的精度以及需要保留
            # 的样本数量，计算出我们需要保
            # 留什么时间之前的样本。
            cutoff = time.time() - SAMPLE_COUNT * prec
            # 获取样本的开始时间，并将其
            # 从字符串转换为整数
            samples = list(map(int, conn.hkeys(hkey)))
            samples.sort()

            # 计算出需要移除的样本数量
            # bisect_right:计算cutoff将被插入到samples的位置
            # right:有重复值时，取右边的位置
            remove = bisect.bisect_right(samples, cutoff)
            if remove:
                conn.hdel(hkey, *samples[:remove])
                # 这个散列可能已经被清空
                if remove == len(samples):
                    try:
                        # 在尝试修改计数器散列之前，对其进行监视
                        pipe.watch(hkey)
                        # 验证计数器散列是否为空，如果是的话
                        # 那么从记录已知计数器的有序集合里面移除它
                        # 计数器散列不为空，继续让它留在记录已知
                        # 计数器的有序集合里面
                        if not pipe.hlen(hkey):
                            pipe.multi()
                            pipe.zrem("known:", hash_key)
                            pipe.execute()
                            # 在删除了一个计数器的情况下
                            # 下次循环可以使用本次循环相同的索引
                            index -= 1
                        else:
                            pipe.unwatch()
                    except redis.exceptions.WatchError:
                        # 有其他程序向这个散列添加了新的数据
                        # 它已经不再是空的了
                        # 继续让它留在记录已知
                        # 计数器的有序集合里面
                        pass

        # 为了让清理操作的执行频率与计数器更新
        # 的频率保持一致，对记录循环次数的变量
        # 以及记录执行时长的变量进行更新。
        passes += 1
        duration = min(int(time.time() - start) + 1, 60)
        # 如果这次循环未耗尽 60 秒，那么在余下的时
        # 间内进行休眠；如果 60 秒已经耗尽，那么休
        # 眠 1 秒以便稍作休息。
        time.sleep(max(60 - duration, 1))
