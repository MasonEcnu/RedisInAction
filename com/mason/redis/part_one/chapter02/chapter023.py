#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from com.mason.redis.helper import can_cache
from com.mason.redis.helper import hash_request


# 网页缓存
# 对于一个不能被缓存的请求，函数将直接生成并返回页面；而对于可以被缓存的请求，
# 函数首先会尝试从缓存里面取出并返回被缓存的页面，如果缓存页面不存在，那么函数会生成页
# 面并将其缓存在 Redis 里面 5 分钟，最后再将页面返回给函数调用者。


def cache_request(conn, request, callback):
    # 不能缓存的，直接调用回调函数
    if not can_cache(conn, request):
        return callback(request)

    # 将请求转换成字符串键
    page_key = "cache:" + hash_request(request)
    content = conn.get(page_key)

    # 如果页面没有被缓存，则生成页面缓存
    if not content:
        content = callback(request)
        # 存储缓存，并设置5分钟的过期时间
        conn.setex(page_key, content, 300)

    return content
