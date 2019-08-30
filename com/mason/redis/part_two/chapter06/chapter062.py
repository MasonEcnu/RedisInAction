#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# 分布式锁
# 获取（acquire）锁
# 操作数据
# 释放（release）锁
# 共享内存数据结构（shared-memory data structure）


# WATCH
# 乐观锁（optimistic locking）

import time
# 下面列出了一些导致锁出现不正确行为的原因，以及锁在不正确运行时的症状。
# 1.持有锁的进程因为操作时间过长而导致锁被自动释放，但进程本身并不知晓这一点，甚
# 至还可能会错误地释放掉了其他进程持有的锁。
# 2.一个持有锁并打算执行长时间操作的进程已经崩溃，但其他想要获取锁的进程不知道哪
# 个进程持有着锁，也无法检测出持有锁的进程已经崩溃，只能白白地浪费时间等待锁被
# 释放。
# 3.在一个进程持有的锁过期之后，其他多个进程同时尝试去获取锁，并且都获得了锁。
# 4.上面提到的第一种情况和第三种情况同时出现，导致有多个进程获得了锁，而每个进程
# 都以为自己是唯一一个获得锁的进程。
import uuid

import redis
from redis import Redis


def acquire_lock(conn: Redis, lock_name, acquire_timeout=10):
    # 128位标识符
    identifier = str(uuid.uuid4())
    end = time.time() + acquire_timeout
    while time.time() < end:
        # 尝试获取锁
        if conn.setnx("lock:" + lock_name, identifier):
            return identifier
        time.sleep(0.001)
    return False


def release_lock(conn, lock_name, identifier):
    pipe = conn.pipeline(True)
    lock_name = "lock:" + lock_name
    while True:
        try:
            pipe.watch(lock_name)
            # 检查进程是否仍然持有锁
            if pipe.get(lock_name) == identifier:
                pipe.multi()
                # 释放锁
                pipe.delete(lock_name)
                pipe.execute()
                return True
            pipe.unwatch()
            break
            # 如果有其他客户端修改了锁，则重试
        except redis.exceptions.WatchError:
            # 那应该是continue吧
            pass
    return False


def purchase_item_with_lock(conn: Redis, buyer_id, item_id, seller_id):
    buyer = "users:%s" % buyer_id
    seller = "users:%s" % seller_id
    item = "%s.%s" % (item_id, seller_id)
    inventory = "inventory:%s" % buyer_id

    # 尝试获取锁
    locked = acquire_lock(conn, "market")
    if not locked:
        return False

    pipe = conn.pipeline(True)
    try:
        pipe.zscore("market:", item)
        pipe.hget(buyer, "funds")
        price, funds = pipe.execute()
        if price is None or price > funds:
            return None

        pipe.hincrby(seller, "funds", int(price))
        pipe.hincrby(buyer, "funds", int(-price))
        pipe.sadd(inventory, item_id)
        pipe.zrem("market:", item)
        pipe.execute()
        return True
    finally:
        release_lock(conn, "market", locked)
