#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time

from com.mason.redis.constant import const
from com.mason.redis_client import redisClient

article_name = "lulala:10086"
time_zset = "time:"
score_zset = "score:"
voted_set = "voted:"
article_id = article_name.partition(":")[-1]

redisClient.delete(article_name)
redisClient.delete(time_zset)
redisClient.delete(score_zset)
redisClient.delete(voted_set + article_id)

curr_time = time.time()
print("curr_time={curr_time}".format(curr_time=curr_time))
redisClient.zadd(time_zset, {article_name: curr_time})
redisClient.zadd(score_zset, {article_name: 0})
redisClient.hset(article_name, "votes", 0)


# 投票
def article_vote(conn, user, article, is_negative=False):
    cutoff = time.time() - const.ONE_WEEK_IN_SECONDS
    # 投票时间为文章发布一周内
    curr_score = conn.zscore(time_zset, article)
    if curr_score < cutoff:
        return
    article_id = article.partition(":")[-1]
    # 如果投的是反对票
    if is_negative:
        if conn.sadd(voted_set + article_id, user):
            # 投反对票减去相应的积分
            conn.zincrby(score_zset, -const.VOTE_SCORES, article)  # zincrby:redis_key, score, key
            # 增加文章的反对票数

            # 不增加列的情况下，可以减去votes字段的值，但是这样可能导致负值出现
            # 对调的话，可以通过相互赋值来操作
            conn.hincrby(article, "n_votes", 1)
    else:
        # 赞成票
        if conn.sadd(voted_set + article_id, user):
            # 增加文章的排序分数
            conn.zincrby(score_zset, const.VOTE_SCORES, article)  # zincrby:redis_key, score, key
            # 增加文章的投票数
            conn.hincrby(article, "votes", 1)


article_vote(redisClient, "mason", article_name)
article_vote(redisClient, "mason10086", article_name, is_negative=True)

print("time:")
print(redisClient.zrange(time_zset, 0, -1, withscores=True))

print("score:")
print(redisClient.zrange(score_zset, 0, -1, withscores=True))

print("voted:" + article_id)
print(redisClient.smembers("voted:" + article_id))

print("article:" + article_id)
print(redisClient.hgetall(article_name))
