#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# 计数信号量
# 计数信号量是一种锁，它可以让用户限制一项资源最多能够同时被多少个进程访问，
# 通常用于限定能够同时使用的资源数量。

import time
# 计数信号量和其他锁的区别在于，当客户端获取锁失败的时候，客户端通常会选择进行等待；
# 而当客户端获取计数信号量失败的时候，客户端通常会选择立即返回失败结果。
import uuid

from redis import Redis


# 由于多主机可能存在时间上的细微差异
# 导致获取信号量失败的问题
# 不公平信号量
# 当各个系统的系统时间并不完全相同的时候，前面介绍的基本信号量就会出现问题：系统时
# 钟较慢的系统上运行的客户端，将能够偷走系统时钟较快的系统上运行的客户端已经取得的信号
# 量，导致信号量变得不公平。

def acquire_semaphore(conn: Redis, sem_name, limit, timeout=10):
    identifier = str(uuid.uuid4())
    now = time.time()

    pipe = conn.pipeline(True)
    pipe.zremrangebyscore(sem_name, "-inf", now - timeout)
    # 尝试获取信号量
    pipe.zadd(sem_name, {identifier: now})
    pipe.zrank(sem_name, identifier)
    # 检查是否成功获取了信号量
    if pipe.execute()[-1] < limit:
        return identifier

    # 获取信号量失败，则删除之前添加的标识符
    conn.zrem(sem_name, identifier)
    return None


def release_semaphore(conn: Redis, sem_name, identifier):
    # 如果信号量已经被正确地释放，那么
    # 返回 True；返回 False 则表示该信
    # 号量已经因为过期而被删除了。
    return conn.zrem(sem_name, identifier)
