#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from com.mason.redis_client import redisClient

# Redis 的有序集合
# 命令    用例和描述
# ZADD
# ZADD key-name score member [score member ...] — 将带有给定分值的成员添加到有序
# 集合里面
# ZREM
# ZREM key-name member [member ...] — 从有序集合里面移除给定的成员，并返回被移除成
# 员的数量
# ZCARD  ZCARD key-name — 返回有序集合包含的成员数量
# ZINCRBY  ZINCRBY key-name increment member — 将 member 成员的分值加上
# increment
# ZCOUNT  ZCOUNT key-name min max — 返回分值介于 min 和
# max 之间的成员数量
# ZRANK  ZRANK key-name member — 返回成员 member 在有序集合中的排名
# ZSCORE  ZSCORE key-name member — 返回成员 member 的分值
# ZRANGE  ZRANGE key-name start stop [WITHSCORES] — 返回有序集合中排名介于
# start 和 stop
# 之间的成员，如果给定了可选的 WITHSCORES 选项，那么命令会将成员的分值也一并返回

redis_zset = "redis_zset"
redisClient.sadd(redis_zset, "a")
redisClient.sadd(redis_zset, "b")
redisClient.sadd(redis_zset, "c")
redisClient.sadd(redis_zset, "d")
print(redisClient.smembers(redis_zset))
print(redisClient.srandmember(redis_zset, 2))
print(redisClient.spop(redis_zset))
# print(redisClient.smove(source, dest, item))

# 命令
# 用例和描述
# ZREVRANK  ZREVRANK key-name member — 返回有序集合里成员
# member 的排名，成员按照分值
# 从大到小排列
# ZREVRANGE  ZREVRANGE key-name start stop [WITHSCORES] — 返回有序集合给定排名范围内
# 的成员，成员按照分值从大到小排列
# ZRANGEBYSCORE  ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count] — 返回
# 有序集合中，分值介于 min 和 max 之间的所有成员
# ZREVRANGEBYSCORE  ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count] —
# 获取有序集合中分值介于 min 和 max 之间的所有成员，并按照分值从大到小的顺序来返
# 回它们
# ZREMRANGEBYRANK  ZREMRANGEBYRANK key-name start stop — 移除有序集合中排名介于 start 和 stop
# 之间的所有成员
# ZREMRANGEBYSCORE  ZREMRANGEBYSCORE key-name min max — 移除有序集合中分值介于 min 和 max 之
# 间的所有成员
# ZINTERSTORE  ZINTERSTORE dest-key key-count key [key ...] [WEIGHTS weight
# [weight ...]] [AGGREGATE SUM|MIN|MAX] — 对给定的有序集合执行类似于集合的
# 交集运算
# ZUNIONSTORE  ZUNIONSTORE dest-key key-count key [key ...] [WEIGHTS weight
# [weight ...]] [AGGREGATE SUM|MIN|MAX] — 对给定的有序集合执行类似于集合的
# 并集运算

redisClient.delete(redis_zset)
