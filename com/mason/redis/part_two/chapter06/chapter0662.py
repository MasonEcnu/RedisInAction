#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 发送日志文件
import os
import time
from collections import deque

from redis import Redis

from com.mason.redis.part_two.chapter06.chapter065 import create_chat, send_message


def copy_logs_to_redis(conn: Redis, path, channel, count=10, limit=2 ** 30, quit_when_done=True):
    bytes_in_redis = 0
    waiting = deque()
    # 创建用于向客户端发送消息的群组
    create_chat(conn, "source", map(str, range(count)), "", channel)
    count = str(count)
    # 遍历所有日志文件
    for logfile in sorted(os.listdir(path)):
        full_path = os.path.join(path, logfile)
        file_size = os.stat(full_path).st_size
        # 如果程序需要更多的空间，那么清除已处理完毕的文件
        while bytes_in_redis + file_size > limit:
            cleaned = _clean(conn, channel, waiting, count)
            if cleaned:
                bytes_in_redis -= cleaned
            else:
                time.sleep(0.25)
        # 将文件上传到Redis
        with open(full_path, "rb") as inp:
            block = " "
            while block:
                block = inp.read(2 ** 17)
                conn.append(channel + logfile, block)
        # 提醒监听者，文件已经准备就绪
        send_message(conn, channel, "source", logfile)

        # 对本地记录的Redis内存占用量相关信息进行更新
        bytes_in_redis += file_size
        waiting.append((logfile, file_size))

    # 所有日志文件已经处理完毕，向监听者报告此事
    if quit_when_done:
        send_message(conn, channel, "source", ":done")

    # 工作完成，清理无用日志
    while waiting:
        cleaned = _clean(conn, channel, waiting, count)
        if cleaned:
            bytes_in_redis -= cleaned
        else:
            time.sleep(0.25)


def _clean(conn: Redis, channel, waiting, count):
    if not waiting:
        return 0
    w0 = waiting[0][0]
    if conn.get(channel + w0 + ":done") == count:
        conn.delete(channel + w0, channel + w0 + ":done")
        return waiting.popleft()[1]
    return 0
