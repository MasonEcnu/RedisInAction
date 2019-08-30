#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# 通讯录自动补全
# 我们将把有序集合里面的所有分值都设置为 0 — 这种做法使得我们可以使用有序集合的
# 另一个特性：当所有成员的分值都相同时，有序集合将根据成员的名字来进行排序；而当所有成
# 员的分值都是 0 的时候，成员将按照字符串的二进制顺序进行排序。
import bisect
import uuid

import redis
from redis import Redis

from com.mason.redis_client import redisClient

valid_characters = "`abcdefghijklmnopqrstuvwxyz{"


def find_prefix_range(prefix: str):
    # [-1:] 取字符串或列表的最后一个元素
    # [:-1] 取到字符串或列表的最后一个元素（不包含最后一个元素）
    # [a，b)前闭后开区间
    pos = bisect.bisect_left(valid_characters, prefix[-1:])
    suffix = valid_characters[(pos or 1) - 1]
    return prefix[:-1] + suffix + '{', prefix + '{'


# print(find_prefix_range("prefix"))


def autocomplete_on_prefix(conn: Redis, guild, prefix):
    # 根据给定的前缀计算出查找的范围
    start, end = find_prefix_range(prefix)
    identifier = str(uuid.uuid4())
    start += identifier
    end += identifier
    zset_name = "members:" + guild

    # 将范围的起始和结束元素添加到有序集合中
    conn.zadd(zset_name, {start: 0, end: 0})
    pipe = conn.pipeline(True)
    items = []
    while 1:
        try:
            pipe.watch(zset_name)
            # 找到开始和结束元素在有序列表中的排名
            start_rank = pipe.zrank(zset_name, start)
            end_rank = pipe.zrank(zset_name, end)

            # 程序最多只会取出 10 个元素
            query_range = min(start_rank + 9, end_rank - 2)
            pipe.multi()
            pipe.zrem(zset_name, start, end)
            pipe.zrange(zset_name, start_rank, query_range)
            items = pipe.execute()[-1]
            break
        except redis.exceptions.WatchError:
            # 如果自动补全集合被其他客户端修改过
            # 则重试
            continue
    # 如果有其他自动补全操作正在执行，那么从获
    # 取到的元素里面移除起始元素和结束元素
    return [item for item in items if "{" not in item]


def join_guild(conn: Redis, guild, user):
    conn.zadd("members:" + guild, {user: 0})


def leave_guild(conn: Redis, guild, user):
    conn.zrem("members:" + guild, user)


guild = "10086"
redisClient.delete("members:" + guild)
join_guild(redisClient, guild, "mason")
join_guild(redisClient, guild, "yahaha")
join_guild(redisClient, guild, "lilei")
join_guild(redisClient, guild, "hmeimei")
join_guild(redisClient, guild, "mmmeee")
join_guild(redisClient, guild, "lulala")
print(autocomplete_on_prefix(redisClient, guild, "mma"))
redisClient.delete("members:" + guild)
