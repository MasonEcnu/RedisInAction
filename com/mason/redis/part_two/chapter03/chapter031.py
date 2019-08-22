#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from com.mason.redis_client import redisClient

# Redis 的字符串就是一个由字节组成的序列
# 字符串可以存储以下 3 种类型的值。
# 字节串（byte string）。
# 整数。
# 浮点数。

redis_str = "redis_str"
print(redisClient.delete(redis_str))
print(redisClient.get(redis_str))  # None
print(redisClient.incr(redis_str))  # 对不存在的键执行自增或者自减时，redis会先初始化一个键，然后认为它的值为0
print(redisClient.incr(redis_str, amount=10))  # 自增自减函数的amount参数可选，默认是1
print(redisClient.decr(redis_str, amount=5))
print(redisClient.get(redis_str))
print(redisClient.set(redis_str, "22"))
print(redisClient.get(redis_str))
print(redisClient.append(redis_str, "23"))  # 尾部追加
print(len(redisClient.get(redis_str)))

# 在使用 SETRANGE 或者 SETBIT 命令对字符串进行写入的时候，如果字符串当前的长度不
# 能满足写入的要求，那么 Redis 会自动地使用空字节（null）来将字符串扩展至所需的长度，然
# 后才执行写入或者更新操作。

print(redisClient.getrange(redis_str, 0, -1))  # 截取key对应的value，包含start和end，闭区间
print(redisClient.setrange(redis_str, 0, "heiha"))  # 将键值从start开始的内容，替换为value
print(redisClient.get(redis_str))

# 在使用 GETRANGE 读取字符串的时候，超出字符串末尾的数据会
# 被视为是空串，而在使用 GETBIT 读取二进制位串的时候，超出字符串末尾的二进制位会被视为是 0。
print(redisClient.getbit(redis_str, 2))
print(redisClient.setbit(redis_str, 2, 0))

# 统计二进制位串里面值为 1 的二进制位的数量，如果给定
# 了可选的 start 偏移量和 end 偏移量，那么只对偏移量指定范围内的二进制位进行统计
print(redisClient.bitcount(redis_str, 0, -1))
# 对一个或多个二进制位串执行包
# 括并（ AND ）、或 （ OR ）、异或（ XOR ）、非 （ NOT ）在内的任意一种按位运算操作（bitwise operation），
# 并将计算得出的结果保存在 dest-key 键里面
# print(redisClient.bitop(redis_str, 2, 0))

print(redisClient.delete(redis_str))
