#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# 刷新信号量
import time

from redis import Redis

from com.mason.redis.part_two.chapter06.chapter0632 import release_fair_semaphore


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
