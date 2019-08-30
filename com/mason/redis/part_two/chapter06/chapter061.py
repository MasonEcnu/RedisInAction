#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# 自动补全(autocomplete)

# 自动补全最近联系人
# （1）如果指定的联系人已经存在于最近联系人列表里面，那么从列表里面移除他。
# （2）将指定的联系人添加到最近联系人列表的最前面。
# （3）如果在添加操作完成之后，最近联系人列表包含的联系人数量超过了 100 个，那么对列
# 表进行修剪，只保留位于列表前面的 100 个联系人
from redis import Redis

from com.mason.redis_client import redisClient


def add_update_contact(conn: Redis, user, contact):
    ac_list = "recent:" + user
    pipe = conn.pipeline(True)
    # 已存在则移除
    pipe.lrem(ac_list, 1, contact)
    # 插入到最前端
    pipe.lpush(ac_list, contact)
    # 修剪
    pipe.ltrim(ac_list, 0, 99)
    pipe.execute()


def remove_contact(conn: Redis, user, contact):
    conn.lrem("recent:" + user, 1, contact)


def fetch_autocomplete_list(conn: Redis, user, prefix):
    # 获取自动补全列表
    candidates = conn.lrange("recent:" + user, 0, -1)
    matches = []
    # 遍历匹配
    for candidate in candidates:
        if str(candidate).lower().startswith(prefix):
            matches.append(candidate)
    return matches


add_update_contact(redisClient, "mason", "lilei")
add_update_contact(redisClient, "mason", "hanmeimei")
add_update_contact(redisClient, "mason", "lulala")
add_update_contact(redisClient, "mason", "lilaohu")
add_update_contact(redisClient, "mason", "yahaha")
add_update_contact(redisClient, "mason", "caonima")
add_update_contact(redisClient, "mason", "nishuosha")
add_update_contact(redisClient, "mason", "woshayemeishuo")
add_update_contact(redisClient, "mason", "huhuhu")
add_update_contact(redisClient, "mason", "???")

print(fetch_autocomplete_list(redisClient, "mason", "l"))
redisClient.delete("recent:mason")
