#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from com.mason.redis_client import redisClient

# Redis 的散列
# HEXISTS
# HEXISTS key-name key — 检查给定键是否存在于散列中
# HKEYS
# HKEYS key-name — 获取散列包含的所有键
# HVALS
# HVALS key-name — 获取散列包含的所有值
# HGETALL
# HGETALL key-name — 获取散列包含的所有键值对
# HINCRBY
# HINCRBY key-name key increment — 将键 key 存储的值加上整数 increment
# HINCRBYFLOAT
# HINCRBYFLOAT key-name key increment — 将键 key 存储的值加上浮点数 increment

redis_hash = "redis_hash"
redisClient.hmset(redis_hash, {"key01": "value01"})
print(redisClient.hkeys(redis_hash))
redisClient.delete(redis_hash)
