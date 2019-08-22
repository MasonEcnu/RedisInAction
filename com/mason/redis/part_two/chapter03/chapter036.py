#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import threading
import time

from com.mason.redis_client import redisClient


# Redis 发布订阅pub/sub
# 命令    用例和描述
# SUBSCRIBE  SUBSCRIBE channel [channel ...] — 订阅给定的一个或多个频道
# UNSUBSCRIBE  UNSUBSCRIBE [channel [channel ...]] — 退订给定的一个或多个频道，如果执行时没
# 有给定任何频道，那么退订所有频道
# PUBLISH  PUBLISH channel message — 向给定频道发送消息
# PSUBSCRIBE  PSUBSCRIBE pattern [pattern ...] — 订阅与给定模式相匹配的所有频道
# PUNSUBSCRIBE  PUNSUBSCRIBE [pattern [pattern ...]] — 退订给定的模式，如果执行时没有给定任何
# 模式，那么退订所有模式

def publisher(n):
    time.sleep(1)
    for i in range(n):
        redisClient.publish("channel", i)
        time.sleep(1)


def rub_pubsub():
    # 启动发送者线程，并发送三条消息
    threading.Thread(target=publisher, args=(3,)).start()
    pubsub = redisClient.pubsub()
    # 订阅指定频道
    pubsub.subscribe(["channel"])
    count = 0
    # 监听订阅频道发送的消息
    for item in pubsub.listen():
        print(item)
        count += 1
        # 在接收到一条订阅反馈消息和三条发布者发送的
        # 消息之后，执行退订操作，停止监听新消息。
        if count == 4:
            pubsub.unsubscribe()
        if count == 5:
            break


# client-output-buffer-limit pubsub

# 1，如果一个客户端订阅了频道，但自己读取消息的速度却不够快的话，
# 那么不断积压的消息会使redis输出缓冲区的体积变得越来越大，
# 这可能使得redis本身的速度变慢，甚至直接崩溃。
# 2，这和数据传输可靠性有关，如果在订阅方断线，
# 那么他将会丢失所有在短线期间发布者发布的消息。

rub_pubsub()
