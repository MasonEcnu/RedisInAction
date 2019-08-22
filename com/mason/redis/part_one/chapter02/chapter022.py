#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time

from com.mason.redis.constant import const


# 使用 Redis 实现购物车
# 每个用户的购物车都是一个散列，这个散列存储了商品ID 与商品订购
# 数量之间的映射。对商品数量进行验证的工作由 Web 应用程序负责，我们要做的则是在商品的订购
# 数量出现变化时，对购物车进行更新：如果用户订购某件商品的数量大于0，那么程序会将这件商品
# 的ID 以及用户订购该商品的数量添加到散列里面，如果用户购买的商品已经存在于散列里面，那么
# 新的订购数量会覆盖已有的订购数量；相反地，如果用户订购某件商品的数量不大于0，那么程序将
# 从散列里面移除该条目。

# items={item:count}
# 可以同时添加多个商品到购物车
def add_to_cart(conn, session, items={}):
    for item in items.keys():
        count = items[item]
        if count <= 0:
            conn.hrem("cart:" + session, item)
        else:
            conn.hset("cart:" + session, item, count)


QUIT_FLAG = False


def clean_full_sessions(conn):
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
            # 删除旧会话的同时，删除该会话对应的购物车记录
            session_keys.append("cart:" + token)

        # 删除键
        conn.delete(*session_keys)  # 删除对应的浏览记录
        conn.hdel("login:", *target_tokens)  # 删除登录记录
        conn.zrem("recent:", *target_tokens)  # 删除最近浏览记录
