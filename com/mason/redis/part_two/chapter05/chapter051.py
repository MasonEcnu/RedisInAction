#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
from datetime import datetime

import redis
from redis import Redis

LOG_LEVEL = {
    logging.DEBUG: "debug",
    logging.INFO: "info",
    logging.WARNING: "waring",
    logging.ERROR: "error",
    logging.CRITICAL: "critical"
}


# LOG_LEVEL.update((name, name) for name in LOG_LEVEL.values())

def log_recent(conn: Redis, name, message, log_level=logging.INFO, pipe=None):
    # 将日志级别，转化为字符串
    log_level = str(LOG_LEVEL.get(log_level, log_level)).lower()
    destination = "recent:%s:%s" % (name, log_level)
    message = time.asctime() + " " + message
    # 通过流水线降低客户端与redis服务器的通信次数
    pipe = pipe or conn.pipeline()
    pipe.lpush(destination, message)
    # 对列表进行修剪，使它只包含最新的100条日志记录
    pipe.ltrim(destination, 0, 99)
    pipe.execute()


def log_common(conn: Redis, name, message, log_level=logging.INFO, timeout=5):
    log_level = str(LOG_LEVEL.get(log_level, log_level)).lower()
    destination = "common:%s:%s" % (name, log_level)
    start_key = destination + ":start"
    pipe = conn.pipeline()
    end = time.time() + timeout
    while time.time() < end:
        try:
            pipe.watch(start_key)
            # 获取当前时间
            now = datetime.utcnow().timetuple()
            # 获取当前小时
            hour_start = datetime(*now[:4]).isoformat()

            # 监听记录小时数的键，确保日志轮换可以正确的执行
            existing = pipe.get(start_key)
            pipe.multi()
            # 如果这个常见日志信息列表记录的是上一个小时的日志
            if existing and existing < hour_start:
                # 那么将这些旧的日志归档
                pipe.rename(destination, destination + ":last")
                pipe.rename(start_key, destination + ":pstart")
                # 更新当前的小时数
                pipe.set(start_key, hour_start)
            elif not existing:
                pipe.set(start_key, hour_start)

            # 对记录日志出现次数的计数器执行自增
            pipe.zincrby(destination, message)
            # log_recent负责记录日志，并执行execute
            log_recent(pipe, name, message, log_level, pipe)
            return
        except redis.exceptions.WatchError:
            # 重试
            continue
