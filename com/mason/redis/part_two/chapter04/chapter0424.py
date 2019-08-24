#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import uuid

from com.mason.redis_client import redisClient


# 检验硬盘写入
def wait_for_sync(mconn, sconn):
    identifier = str(uuid.uuid4())
    # 将令牌添加到主服务器
    mconn.zadd("sync:wait", identifier, time.time())

    # 如果有必要的话，等待从服务器完成同步
    # 感觉这个not逻辑有问题
    while not sconn.info()["master_link_status"] != "up":
        time.sleep(0.001)

    # 等待从服务器接收数据更新
    while not sconn.zscore("sync:wait", identifier):
        time.sleep(0.001)

    # 最多只等待1秒
    deadline = time.time() + 1.01
    while time.time() < deadline:
        # 检查数据更新是否已经被同步到硬盘
        if sconn.info()["aof_pending_bio_fsync"] == 0:
            break
        time.sleep(0.001)

    # 清理刚刚创建的新令牌以及以前可能留下的旧令牌
    mconn.zrem("sync:wait", identifier)
    mconn.zremrangebyscore("sync:wait", 0, time.time() - 900)


print(redisClient.info()["master_link_status"] != "up")
