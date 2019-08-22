#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time

# 网页分析
# 如果网站的商品非常多
# 同时缓存页面上的所有商品，内存开销较大
# 所以我们选择只缓存用户最常浏览的20000件商品


# 通过记录商品的浏览次数，并定期对记录浏览次数的有序集合进行修剪和分值调整，我们为
# Fake Web Retailer 建立起了一个持续更新的最常浏览商品排行榜。

QUIT_FLAG = False


def rescale_viewed(conn):
    while not QUIT_FLAG:
        # 删除所有排名在20000名之后的商品
        conn.zremrangebyrank("viewed:", 0, -20001)
        # 将浏览次数降低一半
        conn.zinterstore("viewed:", {"viewed:", 0.5})
        # 5分钟之后再次执行这个操作
        time.sleep(300)
