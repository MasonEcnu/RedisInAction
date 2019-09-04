#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import time

# 任务队列
from redis import Redis

from com.mason.redis.my_log import get_logger

my_logger = get_logger()


def send_sold_email_via_queue(conn: Redis, seller, item, price, buyer):
    data = {
        "seller_id": seller,
        "item_id": item,
        "price": price,
        "buyer_id": buyer,
        "time": time.time()
    }
    result = ("fetch_data_and_send_sold_email", data)
    conn.rpush("queue:email", json.dumps(result))


QUIT_FLAG = False


class EmailSendError(Exception):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


def fetch_data_and_send_sold_email(*to_send):
    print(to_send)
    pass


def process_sold_email_queue(conn: Redis):
    while not QUIT_FLAG:
        packed = conn.blpop(["queue:email"], timeout=30)
        if not packed:
            continue
        to_send = json.loads(packed[1])
        try:
            fetch_data_and_send_sold_email(to_send)
        except EmailSendError as err:
            my_logger.error("Failed to send sold email: %s, %s", err, to_send)
        else:
            my_logger.info("Send sold email %s", to_send)


# redisClient.delete("queue:email")
# send_sold_email_via_queue(redisClient, "seller", "item", "price", "buyer")


# process_sold_email_queue(redisClient)

# 多个可执行任务
def worker_watch_queue(conn: Redis, queue, callbacks):
    while not QUIT_FLAG:
        # 尝试取出一项执行任务
        packed = conn.blpop([queue], timeout=30)
        if not packed:
            # 队列为空，则重试
            continue

        # 解码任务信息
        name, args = json.loads(packed[1])
        if name not in callbacks:
            # 没有找到任务指定的回调函数，记录错误日志，并重试
            my_logger.error("Unknown callback %s", name)
            continue

        # 执行任务
        callbacks[name](args)


# callbacks = {"fetch_data_and_send_sold_email": fetch_data_and_send_sold_email}
# worker_watch_queue(redisClient, "queue:email", callbacks)


# 任务优先级
# queues:分别按顺序放置高中低优先级的任务key即可
# blpop:将弹出第一个非空列表的第一个元素
def worker_watch_queue_with_priority(conn: Redis, queues, callbacks):
    while not QUIT_FLAG:
        packed = conn.blpop(queues, timeout=30)
        if not packed:
            continue

        name, args = json.loads(packed[1])
        if name not in callbacks:
            my_logger.error("Unknown callback %s", name)
            continue

        callbacks[name](args)
