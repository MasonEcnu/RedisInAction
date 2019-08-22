#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from com.mason.redis_client import redisClient

# Redis 的列表

redis_list = "redis_list"
redisClient.rpush(redis_list, "a")
redisClient.rpush(redis_list, "b")
redisClient.rpush(redis_list, "c")
redisClient.rpush(redis_list, "d")
print(redisClient.lrange(redis_list, 0, -1))
redisClient.ltrim(redis_list, 0, 1)  # 截取列表中[start, end]的内容
print(redisClient.lrange(redis_list, 0, -1))

# 阻塞式的列表弹出命令以及在列表之间移动元素的命令
# 命令
# 用例和描述
# BLPOP  BLPOP key-name [key-name ...] timeout — 从第一个非空列表中弹出位于最左端的元素，
# 或者在 timeout 秒之内阻塞并等待可弹出的元素出现
# BRPOP  BRPOP key-name [key-name ...] timeout — 从第一个非空列表中弹出位于最右端的元素，
# 或者在 timeout 秒之内阻塞并等待可弹出的元素出现
# RPOPLPUSH  RPOPLPUSH source-key dest-key — 从
# source-key 列表中弹出位于最右端的元素，然后
# 将这个元素推入 dest-key 列表的最左端，并向用户返回这个元素
# BRPOPLPUSH  BRPOPLPUSH source-key dest-key timeout — 从 source-key 列表中弹出位于最右端的
# 元素，然后将这个元素推入 dest-key 列表的最左端，并向用户返回这个元素；如果 source-key
# 为空，那么在 timeout 秒之内阻塞并等待可弹出的元素出现

redisClient.delete(redis_list)
