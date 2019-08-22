#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from com.mason.redis.part_one.chapter01.chapter0132 import get_articles


def add_groups(conn, article_id, groups=None):
    if groups is None:
        groups = []
    article = "article" + article_id
    for group in groups:
        conn.sadd("group:" + group, article)


def remove_groups(conn, article_id, groups=None):
    if groups is None:
        groups = []
    article = "article" + article_id
    for group in groups:
        conn.srem("group:" + group, article)


def get_group_articles(conn, group, page, order="score:"):
    key = order + group
    if not conn.exists(key):
        # 求交集，按score高的给出最终结果
        # 默认没有score的集合中的元素score=1
        conn.zinterstore(key, ["group:" + group, order], aggregate="max")
        conn.expire(key, 60)
    return get_articles(conn, page, key)
