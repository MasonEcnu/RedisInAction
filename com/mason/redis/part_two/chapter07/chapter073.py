#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 分析并执行搜索
import re

from com.mason.redis.part_two.chapter07.chapter071 import STOP_WORDS

# 用于查找需要的单词、不需要的单词以及同义词的正则
QUERY_RE = re.compile("[+-]?[a-z']{2,}")


def parse(query):
    # 差集
    unwanted = set()
    # 交集
    all = []
    # 同义词
    current = set()

    # 遍历查询中的所有单词
    for match in QUERY_RE.finditer(query.lower()):
        word = match.group()
        prefix = word[:1]
        # 检查单词前缀
        if prefix in "+-":
            word = word[1:]
        else:
            prefix = None

        word = word.strip("'")
        if len(word) < 2 or word in STOP_WORDS:
            continue

        if prefix == "-":
            unwanted.add(word)
            continue

        # 如果同义词集合非空
        # 遇到一个不带+号前缀的单词
        # 那么创建一个新的同义词集合
        if current and not prefix:
            all.append(list(current))
            current = set()

        current.add(word)

    # 把剩余的单词都放到交集计算集合里
    if current:
        all.append(list(current))

    return all, list(unwanted)


str = "connect +connection +disconnect +disconnection chat -proxy -proxies"
print(parse(str))
