#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 消息拉取
import json
import time

from redis import Redis

from com.mason.redis.part_two.chapter06.chapter062 import acquire_lock, release_lock
from com.mason.redis_client import redisClient


def send_message_single(conn: Redis, receiver, sender, msg):
    data = {
        "sender": sender,
        "msg": msg,
        "time": time.time().__int__()
    }
    conn.rpush("mailbox:" + receiver, json.dumps(data))


def receive_message_single(conn: Redis, receiver):
    if conn.llen("mailbox:" + receiver) > 0:
        return conn.lpop("mailbox:" + receiver)
    return None


# send_message_single(redisClient, "mason", "lily", "hello world")
# result = receive_message_single(redisClient, "mason")
# if result:
#     data = json.loads(result)
#     print("%s, %s, %s" % (data["sender"], data["msg"], data["time"]))

# 多接收者消息的发送与订阅替代品
# 创建群组聊天会话


def create_chat(conn: Redis, sender, recipients: list, message, chat_id=None):
    # 获取新的群组Id
    chat_id = chat_id or str(conn.incr("ids:chat:"))

    # 创建一个由用户和分值组成的字典
    # 字典里的信息将被添加到有序集合里
    recipients.append(sender)
    # 生成器表达式（generator expression）
    recipients_map = dict((r, 0) for r in recipients)

    pipe = conn.pipeline(True)
    # 将所有参数群聊的用户添加到有序集合里
    pipe.zadd("chat:" + chat_id, recipients_map)
    # 初始化已读有序集合
    for rec in recipients:
        pipe.zadd("seen:" + rec, {chat_id: 0})
    pipe.execute()
    # 发送消息
    return send_message(conn, chat_id, sender, message)


# 发送消息
def send_message(conn: Redis, chat_id, sender, message):
    identifier = acquire_lock(conn, "chat:" + chat_id)
    if not identifier:
        raise Exception("Couldn't get the lock")

    try:
        mid = conn.incr("ids:" + chat_id)
        send_time = time.time()
        data = json.dumps({
            "id": mid,
            "send_time": send_time,
            "sender": sender,
            "msg": message
        })

        conn.zadd("msgs:" + chat_id, {data: mid})
    finally:
        release_lock(conn, "chat:" + chat_id, identifier)

    return chat_id


# 获取消息
def fetch_pending_messages(conn: Redis, recipient):
    seen = conn.zrange("seen:" + recipient, 0, -1, withscores=True)
    pipe = conn.pipeline(True)
    # 获取所有未读消息
    for chat_id, seen_id in seen:
        pipe.zrangebyscore("msgs:" + chat_id, seen_id + 1, "inf")

    # 将取出的所有未读消息进行封装
    chat_info = list(zip(seen, pipe.execute()))
    for i, ((chat_id, seen_id), messages) in enumerate(chat_info):
        if not messages:
            continue
        messages[:] = map(json.loads, messages)
        # 使用最新收到的消息来更新群组有效集合
        seen_id = messages[-1]["id"]
        conn.zadd("chat:" + chat_id, {recipient: seen_id})

        # 找出那些所有人都已经阅读过的信息
        # chat:+chat_id：中是按照已阅读的chat_id正序的，所以已读消息的最小id就是第一个
        min_id = conn.zrange("chat:" + chat_id, 0, 0, withscores=True)

        # 更新已读消息有序集合
        pipe.zadd("seen:" + recipient, {chat_id: seen_id})
        if min_id:
            # 删除那些已经被所有人阅读过的消息
            pipe.zremrangebyscore("msgs:" + chat_id, 0, min_id[0][1])
        chat_info[i] = (chat_id, messages)
    pipe.execute()
    return chat_info


# 加入和离开群组
def join_chat(conn: Redis, chat_id, user):
    # 获取最新的群里消息id
    message_id = int(conn.get("ids:" + chat_id))
    pipe = conn.pipeline(True)
    # 用户加入群组
    pipe.zadd("chat:" + chat_id, {user: message_id})
    # 将最新消息加入到已读列表
    pipe.zadd("seen:" + user, {chat_id: message_id})
    pipe.execute()


def leave_chat(conn: Redis, chat_id, user):
    pipe = conn.pipeline(True)
    chat_id = str(chat_id)
    # 从群组中移除
    pipe.zrem("chat:" + chat_id, user)
    # 从已读消息中移除
    pipe.zrem("seen:" + user, chat_id)
    pipe.zcard("chat:" + chat_id)

    # 聊天室已经空了
    if pipe.execute()[-1]:
        pipe.delete("msgs:" + chat_id)
        pipe.delete("ids:" + chat_id)
        pipe.execute()
    else:
        # 找出并删除那些被所有成员阅读过的消息
        oldest = conn.zrange("chat:" + chat_id, 0, 0, withscores=True)
        conn.zremrangebyscore("msgs:" + chat_id, 0, oldest[0][1])


sender = "mason"
recipients = ["lily", "yahaha"]
# create_chat(redisClient, sender, recipients, "Hello World!")
msg = fetch_pending_messages(redisClient, sender)
print(msg)
# msg = fetch_pending_messages(redisClient, "lily")
leave_chat(redisClient, 1, sender)
