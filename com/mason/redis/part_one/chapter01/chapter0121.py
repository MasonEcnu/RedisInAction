#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from com.mason.redis_client import redisClient

# redis中的字符串
print("redis中的字符串")
redis_str = "redis_str"
redisClient.set(redis_str, "male")
print(redisClient.get(redis_str))
redisClient.delete(redis_str)
print(redisClient.get(redis_str))

# redis中的列表
print("redis中的列表")
redis_list = "redis_list"
redisClient.delete(redis_list)
redisClient.lpush(redis_list, "lilei")  # 从左插入
redisClient.lpush(redis_list, "hanmeimei")
print(redisClient.lrange(redis_list, 0, -1))

redisClient.rpush(redis_list, "lulala")  # 从右插入
redisClient.rpush(redis_list, "huhaha")
print(redisClient.lrange(redis_list, 0, -1))  # 获取全部，-1表示最后一个只的index

print(redisClient.lpop(redis_list))  # 从左边弹出
print(redisClient.llen(redis_list))  # 取list长度
print(redisClient.lindex(redis_list, -1))  # 取出列表最后一个元素

# redis中的集合
print("redis中的集合")
redis_set = "redis_set"
redisClient.delete(redis_set)
redisClient.sadd(redis_set, "item1")  # 集合添加
redisClient.sadd(redis_set, "item2")
print(redisClient.smembers(redis_set))  # 集合打印全部
print(redisClient.sismember(redis_set, "item1"))  # 集合判断是否属于
print(redisClient.sismember(redis_set, "item3"))
print(redisClient.srem(redis_set, "item3"))  # 集合移除

# redis中的散列
print("redis中的散列")
redis_hash = "redis_hash"
redisClient.hmset(redis_hash, {"key1": "value1"})  # 散列设置值，key重复会覆盖
redisClient.hmset(redis_hash, {"key2": "value2"})
redisClient.hmset(redis_hash, {"key1": "value3"})
print(redisClient.hgetall(redis_hash))  # 散列获取全部
print(redisClient.hget(redis_hash, "key2"))  # 散列获取指定key
print(redisClient.hget(redis_hash, "key4"))
print(redisClient.hdel(redis_hash, "key1"))  # 散列删除指定key
print(redisClient.hgetall(redis_hash))
print(redisClient.hlen(redis_hash))  # 散列获取长度

# redis中的有序集合
print("redis中的有序集合")
redis_sorted_set = "redis_sorted_set"
redisClient.zadd(redis_sorted_set, {"key1": 100})
redisClient.zadd(redis_sorted_set, {"key2": 101})
print(redisClient.zrange(redis_sorted_set, 0, 100, withscores=True))
redisClient.zincrby(redis_sorted_set, 1, "key1")
redisClient.zrem(redis_sorted_set, "key2")
print(redisClient.zscore(redis_sorted_set, "key1"))
print(redisClient.zrange(redis_sorted_set, 0, 100, withscores=True))
print(redisClient.zrangebyscore(redis_sorted_set, 0, 100, withscores=True))
print(redisClient.zrangebylex(redis_sorted_set, min="-", max="[key1"))  # lexicographical:字母顺序
