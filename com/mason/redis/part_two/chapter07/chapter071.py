#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 建索引（indexing）
# 反向索引（inverted indexes）
# 语法分析（parsing）和标记化（tokenization）
import json
import re

# 非用词
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
