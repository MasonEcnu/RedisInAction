#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from com.mason.redis_client import redisClient

# Redis 其他命令

# 排序 sort
# 命令    用例和描述
# SORT
# SORT source-key [BY pattern] [LIMIT offset count] [GET pattern [GET
# pattern ...]] [ASC|DESC] [ALPHA] [STORE dest-key] — 根据给定的选项，对输入
# 列表、集合或者有序集合进行排序，然后返回或者存储排序的结果

sort_input = "sort_input"
redisClient.rpush(sort_input, 23, 13, 32, 44, 31)
print(redisClient.lrange(sort_input, 0, -1))
# 默认是升序的
print(redisClient.sort(sort_input))
# 降序
print(redisClient.sort(sort_input, desc=True))
# 字母顺序
print(redisClient.sort(sort_input, alpha=True))
# 将某个散列作为权重
redisClient.hset("d-23", "field", 5)
redisClient.hset("d-13", "field", 1)
redisClient.hset("d-32", "field", 8)
redisClient.hset("d-44", "field", 9)
redisClient.hset("d-31", "field", 3)
# 将散列的域field作为权重，对sort_input进行排序
print(redisClient.sort(sort_input, by="d-*->field"))
print(redisClient.sort(sort_input, by="d-*->field", get="d-*->field"))
redisClient.delete(sort_input)
redisClient.delete("d-23")
redisClient.delete("d-13")
redisClient.delete("d-32")
redisClient.delete("d-44")
redisClient.delete("d-31")
