#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# 细粒度锁
# 只锁要购买的目标商品

# 带有超时限制特性的锁
import math
import time
import uuid

from redis import Redis


def acquire_lock_with_timeout(conn: Redis, lock_name, acquire_timeout=10, lock_timeout=10):
    identifier = str(uuid.uuid4())
    lock_name = "lock:" + lock_name
    # 大于等于lock_timeout的最小整数
    lock_timeout = int(math.ceil(lock_timeout))
    end = time.time() + acquire_timeout
    while time.time() > end:
        # 获取锁，并设置过期时间
        if conn.setnx(lock_name, identifier):
            conn.expire(lock_name, lock_timeout)
            return identifier
        # 检查过期时间，必要时更新
        elif not conn.ttl(lock_name):
            conn.expire(lock_name, lock_timeout)

        time.sleep(0.001)
    return False


print(int(math.ceil(-1.1)))
