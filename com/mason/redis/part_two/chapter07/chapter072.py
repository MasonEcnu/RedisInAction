#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 有序索引

# 使用有序集合对搜索结果进行排序
import uuid

from redis import Redis

from com.mason.redis.part_two.chapter07.chapter071 import parse_and_search


def search_and_zsort(conn: Redis, query, id=None, ttl=300, update=1, vote=0, start=0, num=20, desc=True):
    if id and not conn.expire(id, ttl):
        id = None

    if not id:
        id = parse_and_search(conn, query, ttl=ttl)

        scored_search = {
            "id": 0,
            "sort:update": update,
            "sort:votes:": vote
        }

        id = zintersect(conn, scored_search, ttl)

    pipe = conn.pipeline(True)
    pipe.zcard("idx:" + id)
    if desc:
        pipe.zrevrange("idx:" + id, start, start, + num - 1)
    else:
        pipe.zrange("idx:" + id, start, start + num - 1)

    results = pipe.execute()
    return results[0], results[1], id


def _zset_common(conn: Redis, method, scores, ttl=30, **kw):
    id = str(uuid.uuid4())
    execute = kw.pop("_execute", True)
    pipe = conn.pipeline(True) if execute else conn
    for key in scores.keys():
        scores["idx:" + key] = scores.pop(key)

    getattr(pipe, method)("idx:" + id, scores, **kw)
    pipe.expire("idx:" + id, ttl)
    if execute:
        pipe.execute()

    return id


def zintersect(conn: Redis, items, ttl=30, **kw):
    return _zset_common(conn, "zinterstore", dict(items), ttl, **kw)


def zunion(conn: Redis, items, ttl=30, **kw):
    return _zset_common(conn, "zunionstore", dict(items), ttl, **kw)

# 暂时到此为止
