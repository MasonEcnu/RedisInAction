#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import threading
import time

from com.mason.redis.constant import const
from com.mason.redis_client import redisClient


# Redis 其他命令

# Redis 事务
# 在 Redis 里面，被 MULTI 命令和 EXEC 命令包围的所有命
# 令会一个接一个地执行，直到所有命令都执行完毕为止。
# 当 Redis 从一个客户端那里接收到 MULTI 命令时，
# Redis 会将这个客户端之后发送的所有命令都放入到一个队列里面，直到这个客户端发送 EXEC
# 命令为止，然后 Redis 就会在不被打断的情况下，一个接一个地执行存储在队列里面的命令

# 无事务
def notrans():
    # 对计数器进行自增
    print(redisClient.incr("notrans:"))
    time.sleep(0.1)
    # 对计数器进行自减
    redisClient.incr("notrans:", -1)


# 带事务
def trans():
    pipeline = redisClient.pipeline()
    # 对计数器进行自增
    pipeline.incr("trans:", 1)
    time.sleep(0.1)
    # 对计数器进行自减
    pipeline.incr("trans:", -1)
    print(pipeline.execute())


if 1:
    # 启动三个线程来执行未被事务包裹的notrans()
    for i in range(3):
        threading.Thread(target=trans).start()
    time.sleep(0.5)


# get_articles:事务改进
# 获取指定页码的文章列表
def get_articles(conn, page, order="score:"):
    start = (page - 1) * const.ARTICLES_PER_PAGE
    end = start + const.ARTICLES_PER_PAGE - 1

    # 返回order中，start到end的内容，按score倒序
    ids = conn.zrevrange(order, start, end)

    pipeline = conn.pipeline()
    # Prepare the HGETALL calls on the pipeline
    map(pipeline.hgetall, ids)
    articles = []
    # Execute the pipeline and add ids to the article
    for article_id, article_data in zip(ids, pipeline.execute()):
        article_data[article_id] = article_id
        articles.append(article_data)

    return articles


# 投票事务改进
def article_vote(conn, user, article):
    cutoff = time.time() - const.ONE_WEEK_IN_SECONDS
    # 投票时间为文章发布一周内
    posted = conn.zscore("time:", article)
    if posted < cutoff:
        return
    article_id = article.partition(":")[-1]
    pipeline = conn.pipeline()
    pipeline.sadd("voted:" + article_id, user)
    # Set the expiration time if we shouldn't have actually added the vote to the SET
    pipeline.expire("voted:" + article_id, int(posted - cutoff))
    if pipeline.execute()[0]:
        # 增加文章的排序分数
        pipeline.zincrby("score:", const.VOTE_SCORES, article)  # zincrby:redis_key, score, key
        # 增加文章的投票数
        pipeline.hincrby(article, "votes", 1)
        # We could lose our connection between the SADD/EXPIRE and ZINCRBY/HINCRBY, so the vote may not count,
        # but that is better than it partially counting by failing between the ZINCRBY/HINCRBY calls
        pipeline.execute()
