#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
from urllib.parse import parse_qs
from urllib.parse import urlparse


# 是否可以缓存
def can_cache(conn, request):
    # 尝试从页面里面取出商品ID
    item_id = extract_item_id(request)
    # 检查这个页面能否被缓存以及这个页面是否为商品页
    if not item_id or is_dynamic(request):
        return False
    # 取的商品的浏览次数排名
    rank = conn.zrank("viewed:", item_id)
    # 根据商品的浏览次数排名来判断是否需要缓存这个页面
    return rank is not None and rank < 10000


# 提取itemId
def extract_item_id(request):
    parsed = urlparse(request)
    query = parse_qs(parsed.query)
    return (query.get('item') or [None])[0]


# 是否动态网页
def is_dynamic(request):
    parsed = urlparse(request)
    query = parse_qs(parsed.query)
    return "_" in query


# 计算request的hash值
def hash_request(request):
    return str(hash(request))


# 辅助测试类
class Inventory(object):
    def __init__(self, cid):
        self.id = cid

    @classmethod
    def get(cls, cid):
        return Inventory(cid)

    def to_dict(self):
        return {"id": self.id, "data": "data to cache...", "cached:": time.time()}
