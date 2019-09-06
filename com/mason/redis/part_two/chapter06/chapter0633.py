#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# 刷新信号量
import time

from redis import Redis

from com.mason.redis.part_two.chapter06.chapter062 import acquire_lock, release_lock
from com.mason.redis.part_two.chapter06.chapter0632 import release_fair_semaphore, acquire_fair_semaphore


def refresh_fair_semaphore(conn: Redis, sem_name, identifier):
    # 更新客户端持有的信号量
    # 主要是用于更新过期时间
    if conn.zadd(sem_name, identifier, time.time()):
        # 更新成功，告知调用者，客户端已经失去了信号量
        release_fair_semaphore(conn, sem_name, identifier)
        return False
    else:
        # 客户端仍然持有信号量
        return True


# 消除竞争条件
def acquire_semaphore_with_lock(conn: Redis, sem_name, limit, timeout=10):
    identifier = acquire_lock(conn, sem_name, acquire_timeout=0.01)
    if identifier:
        try:
            return acquire_fair_semaphore(conn, sem_name, limit, timeout)
        finally:
            release_lock(conn, sem_name, identifier)

# 以下是之前介绍过的各个信号量实现的优缺点
# 1.如果你对于使用系统时钟没有意见，也不需要对信号量进行刷新，并且能够接受信号量
# 的数量偶尔超过限制，那么可以使用我们给出的第一个信号量实现。
# 2.如果你只信任差距在一两秒之间的系统时钟，但仍然能够接受信号量的数量偶尔超过限
# 制，那么可以使用第二个信号量实现。
# 3.如果你希望信号量一直都具有正确的行为，那么可以使用带锁的信号量实现来保证正确性。
