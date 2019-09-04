#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 延迟队列
# 实现方案：
# 1.在任务信息中包含任务的执行时间，如果工作进程发现任务的执行时间尚未来临，那么
# 它将在短暂等待之后，把任务重新推入队列里面。
# 2.工作进程使用一个本地的等待列表来记录所有需要在未来执行的任务，并在每次进行
# while 循环的时候，检查等待列表并执行那些已经到期的任务。
# 3.把所有需要在未来执行的任务都添加到有序集合里面，并将任务的执行时间设置为分值，
# 另外再使用一个进程来查找有序集合里面是否存在可以立即被执行的任务，如果有的话，
# 就从有序集合里面移除那个任务，并将它添加到适当的任务队列里面。
import json
import time
import uuid

from redis import Redis

from com.mason.redis.my_log import get_logger
from com.mason.redis.part_two.chapter06.chapter062 import acquire_lock, release_lock

my_logger = get_logger()

QUIT_FLAG = False


def execute_later(conn: Redis, queue, name, args, delay=0):
    identifier = str(uuid.uuid4())
    task = json.dumps([identifier, queue, name, args])
    if delay > 0:
        conn.zadd("delayed:", {task: time.time() + delay})
    else:
        conn.rpush("queue:" + queue, task)
    return identifier


def poll_queue(conn: Redis):
    while not QUIT_FLAG:
        task = conn.zrange("delayed:", 0, 0, withscores=True)
        if not task or task[0][1] > time.time():
            time.sleep(0.01)
            continue

        task = task[0][0]
        identifier, queue, function, args = json.loads(task)

        locked = acquire_lock(conn, identifier)
        if not locked:
            continue

        if conn.zrem("delayed:", task):
            conn.rpush("queue:" + queue, task)

        release_lock(conn, identifier, locked)
