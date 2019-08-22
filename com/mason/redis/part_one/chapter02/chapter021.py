#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time

from com.mason.redis.constant import const


# 对于用来登录的 cookie，有两种常见的方法可以将登录信息存储在 cookie 里面：
# 一种是签名（signed）cookie，
# 另一种是令牌（token）cookie。

# 要检查一个用户是否已经登录，需要根据给定的令牌来查找与之对应的用户，
# 并在用户已经登录的情况下，返回该用户的 ID


def check_token(conn, token):
    # 尝试获取并返回令牌对应的用户
    return conn.hget("login:", token)


# 用户每次浏览页面的时候，程序都会对用户存储在登录散列里面的信息进行更新，并将用户的令牌和
# 当前时间戳添加到记录最近登录用户的有序集合里面；如果用户正在浏览的是一个商品页面，
# 那么程序还会将这个商品添加到记录这个用户最近浏览过的商品的有序集合里面，并在被记
# 录商品的数量超过 25 个时，对这个有序集合进行修剪。

def update_token(conn, token, user, item=None):
    timestamp = time.time()
    # 维持令牌与已登录用户之间的映射
    conn.hset("login:", token, user)
    # 记录令牌最后一次出现的时间
    conn.zadd("recent:", {token: timestamp})
    if item:
        # 记录用户浏览过的商品
        conn.zadd("viewed:" + token, {item: timestamp})
        # 移除旧的记录，保留最多25个浏览记录
        conn.zremrangebyrank("viewed:" + token, 0, -26)
        # 被浏览最多的商品，排在最前面
        # 因为zset默认是升序的
        conn.zincrby("viewed:", item, -1)


# 为了限制会话数据的数量，我们决定只保存最新的 1000 万个会话。
# 清理旧会话的程序由一个循环构成，这个循环每次执行的时候，都会检查存储最近登录令牌的有序集
# 合的大小，如果有序集合的大小超过了限制，那么程序就会从有序集合里面移除最多 100 个
# 最旧的令牌，并从记录用户登录信息的散列里面，移除被删除令牌对应的用户的信息，并对
# 存储了这些用户最近浏览商品记录的有序集合进行清理。与此相反，如果令牌的数量未超过
# 限制，那么程序会先休眠 1 秒，之后再重新进行检查。

QUIT_FLAG = False


def clean_sessions(conn):
    while not QUIT_FLAG:
        # 找出目前存储的令牌数量
        token_size = conn.zcard("recent:")
        # 如果令牌数量未超过限制，则休眠后检查
        if token_size <= const.SESSION_LIMIT:
            time.sleep(1)
            continue
        # 每次移除最多100个旧令牌
        end_index = min(token_size - const.SESSION_LIMIT, 100)
        target_tokens = conn.zrange("recent:", 0, end_index - 1)
        session_keys = []
        # 构建删除目标键名
        for token in target_tokens:
            session_keys.append("viewed:" + token)

        # 删除键
        conn.delete(*session_keys)  # 删除对应的浏览记录
        conn.hdel("login:", *target_tokens)  # 删除登录记录
        conn.zrem("recent:", *target_tokens)  # 删除最近浏览记录
