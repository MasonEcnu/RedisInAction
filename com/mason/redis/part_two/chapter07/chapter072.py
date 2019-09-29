#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 基本搜索操作
import uuid

from redis import Redis


def _set_common(conn: Redis, method, names, ttl=30, execute=True):
    # 创建一个临时标识符
    id = str(uuid.uuid4())
    pipe = conn.pipeline(True) if execute else conn
    # 给要查找的单词加前缀
    names = ["idx:" + name for name in names]
    # 为将要执行的集合操作设置响应的参数
    getattr(pipe, method)("idx:" + id, *names)
    # 搜索结果自动删除
    pipe.expire("idx:" + id, ttl)
    if execute:
        # 执行
        pipe.execute()
    return id


def intersect(conn: Redis, items, ttl=30, _execute=True):
    return _set_common(conn, "sinterstore", items, ttl, _execute)


def union(conn: Redis, items, ttl=30, _execute=True):
    return _set_common(conn, "sunionstore", items, ttl, _execute)


def difference(conn: Redis, items, ttl=30, _execute=True):
    return _set_common(conn, "sdiffstore", items, ttl, _execute)
