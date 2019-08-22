#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from com.mason.redis.constant import const
from com.mason.redis_client import redisClient


#
#
# # 文章发布
# def post_article(conn, user, title, link):
#     article_id = str(conn.incr("article:"))
#     voted = "voted:" + article_id
#     # 自己不能给自己投票
#     conn.sadd(voted, user)
#     # 设置过期时间
#     conn.expire(voted, const.ONE_WEEK_IN_SECONDS)
#
#     now = time.time()
#     article = "article:" + article_id
#     print("post_article:{article_name}".format(article_name=article))
#     # 存储文章的内容
#     conn.hmset(article, {
#         "title": title,
#         "link": link,
#         "poster": user,
#         "time": now,
#         "votes:": 1,
#         "n_votes:": 0
#     })
#
#     # 设置文章的初始分数--这样岂不是，越靠后发布，分数越高？
#     conn.zadd("score:", {article: now + const.VOTE_SCORES})
#     conn.zadd("time:", {article: now})
#
#     return article_id
#
#
# article_id = post_article(redisClient, "mason", "学习redis", "www.baidu.com")
# print(article_id)
# article = "article:" + article_id
# print(article)
# print(redisClient.hgetall(article))


# 获取指定页码的文章列表
def get_articles(conn, page, order="score:"):
    start = (page - 1) * const.ARTICLES_PER_PAGE
    end = start + const.ARTICLES_PER_PAGE - 1

    # 返回order中，start到end的内容，按score倒序
    ids = conn.zrevrange(order, start, end)
    articles = []
    for article_id in ids:
        article_data = conn.hgetall(article_id)
        article_data[article_id] = article_id
        articles.append(article_data)

    return articles


articles = get_articles(redisClient, 1)

for article in articles:
    print(article)
