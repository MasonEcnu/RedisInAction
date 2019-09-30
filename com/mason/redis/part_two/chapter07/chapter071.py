#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 建索引（indexing）
# 反向索引（inverted indexes）
# 语法分析（parsing）和标记化（tokenization）
import json
import re
# 非用词
import uuid

from redis import Redis

STOP_WORDS = set('''I able about across after all almost also am among an 
and any are as at be because been but by can cannot 
could dear did do does either else ever every for from get got had 
has have he her hers him his how however if in into is it its just least let like
likely may me might most must my neither no nor not of off often on
only or other our own rather said say says she should since so some
than that the their them then there these they this tis to too twas us 
wants was we were what when where which while who whom why will with 
would yet you your'''.split())

# 单词正则
# 英文小写+不少于两个字符
WORDS_RE = re.compile("[a-z']{2,}")


# 生成文章的关键字
def tokenize(content):
    words = set()
    # 正则匹配--文章中的
    for match in WORDS_RE.finditer(content.lower()):
        # 剔除所有位于单词前后的单引号
        word = match.group().strip("'")
        # 保留至少两个字符的单词
        if len(word) >= 2:
            words.add(word)
    return words - STOP_WORDS


def clear_index(conn, doc_id, old_doc_words):
    old_doc_words = json.loads(old_doc_words)
    pipe = conn.pipeline(True)
    for word in old_doc_words:
        # 从反向索引集合中删除
        pipe.srem("idx:" + word, doc_id)
    return pipe.execute()


def index_document(conn: Redis, doc_id, content):
    # 如果id对应的文章关键词发生变化
    # 则重新建立反向索引
    new_doc_words = tokenize(content)
    old_doc_words = conn.get("doc_words:" + doc_id)
    # 如果旧的关键字不为空，则清理掉
    if old_doc_words:
        clear_index(conn, doc_id, old_doc_words)
    conn.set("doc_words:" + doc_id, json.dumps(list(new_doc_words)))

    # 对内容进行标记处理
    # 并取得产生的关键字
    pipe = conn.pipeline(True)
    for word in new_doc_words:
        # 添加到反向索引集合
        pipe.sadd("idx:" + word, doc_id)

    return len(pipe.execute())


# 反向索引虽然对关键字搜索友好
# 但是对删除却不够友好
# str = json.dumps(list(STOP_WORDS))
# obj = json.loads(str)
# print(obj)

# 基本搜索操作

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


# 分析并执行搜索
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


# str = "connect +connection +disconnect +disconnection chat -proxy -proxies"
# print(parse(str))


def parse_and_search(conn: Redis, query, ttl=30):
    # 语法分析
    all, unwanted = parse(query)
    if not all:
        return None

    to_intersect = []
    # 遍历同义词表
    for syn in all:
        # 如果同义词表不止一个单词，执行并集操作
        if len(syn) > 2:
            to_intersect.append(union(conn, syn, ttl=ttl))
        # 如果同义词表只有一个单词，就用这个
        else:
            to_intersect.append(syn[0])

    # 如果并集计算结果集大于1，则执行交集操作
    if len(to_intersect) > 1:
        intersect_result = intersect(conn, to_intersect, ttl=ttl)
    else:
        # 如果结果只有一个，则将其作为结果
        intersect_result = to_intersect[0]

    # 如果不需要的单词不为空
    # 则执行差集操作
    if unwanted:
        unwanted.insert(0, intersect_result)
        return difference(conn, unwanted, ttl=ttl)

    return intersect_result


# 对搜索结果进行排序

def search_and_sort(conn: Redis, query, id=None, ttl=300, sort="-updated", start=0, num=20):
    # 升序还是降序
    desc = sort.startswith("-")
    sort = sort.lstrip("-")
    by = "kb:doc:*->" + sort
    # 排序方式
    # 如果没有指定，则采用字母顺序
    alpha = sort not in ("updated", "id", "created")
    # 如果用户没有指定搜索结果，或者结果已经过期
    # 则执行一次新的搜索操作
    if id and not conn.expire(id, ttl):
        id = None
    if not id:
        id = parse_and_search(conn, query, ttl=ttl)

    pipe = conn.pipeline(True)
    pipe.scard("idx:" + id)
    # 排序
    pipe.sort("idx:" + id, by=by, alpha=alpha, desc=desc, start=start, num=num)
    results = pipe.execute()

    return results[0], results[1], id
