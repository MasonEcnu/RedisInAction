#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time

import redis
from redis import Redis


# Redis事务
# 二阶提交（two-phase commit）
# 这种“一次性发送多个命令，然后等待所有回复出现”
# 的做法通常被称为流水线（pipelining），它可以通过减少客户
# 端与 Redis 服务器之间的网络通信次数来提升 Redis 在执行多个命令时的性能。


# 市场上架商品
def list_item(conn: Redis, item_id, seller_id, price):
    inventory = "inventory:%s" % seller_id
    item = "%s.%s" % (item_id, seller_id)
    end = time.time() + 5
    pipe = conn.pipeline()
    # 重试5秒
    while time.time() < end:
        try:
            # 监听卖方包裹
            pipe.watch(inventory)
            # 如果指定道具不在包裹中，那么停止监听，并返回空值
            if not pipe.sismember(inventory, item_id):
                pipe.unwatch()
                return None

            # 开始执行事务
            pipe.multi()
            # 把被销售的商品，添加到市场里
            pipe.zadd("market:", item, price)
            # 从玩家包裹移除代售道具
            pipe.srem(inventory, item_id)
            pipe.execute()
            return True
        except redis.exceptions.WatchError:
            # 用户包裹已经发生了变化，重试
            print("redis.exceptions.WatchError")
            pass
        return False


# 购买商品
# buy_price:点击购买操作时，商品的价格
def purchase_item(conn: Redis, buyer_id, item_id, seller_id, buy_price):
    buyer = "users:%s" % buyer_id
    seller = "users:%s" % seller_id
    item = "%s.%s" % (item_id, seller_id)
    # 买家包裹
    inventory = "inventory:%s" % buyer_id
    # 重试10秒
    end = time.time() + 10
    pipe = conn.pipeline()

    while time.time() < end:
        try:
            pipe.watch("market:", buyer)

            curr_price = pipe.zscore("market:", item)
            funds = int(pipe.hget(buyer, "funds"))
            # 如果商品价格发生变化，或者买家没有足够的钱购买
            if buy_price != curr_price or curr_price > funds:
                pipe.unwatch()
                return None

            pipe.multi()
            # 卖家增加收入
            pipe.hincrby(seller, "funds", int(curr_price))
            # 买家减少资金
            pipe.hincrby(buyer, "funds", int(-curr_price))
            # 买家包裹添加道具
            pipe.sadd(inventory, item_id)
            # 从商场移除商品
            pipe.zrem("market:", item)
            # 执行事务
            pipe.execute()
            return True
        except redis.exceptions.WatchError:
            pass

    return False


# 下架商品
def cancel_item(conn: Redis, owner_id, item_id):
    item = "%s.%s" % (item_id, owner_id)
    # 买家包裹
    inventory = "inventory:%s" % owner_id
    # 重试5秒
    end = time.time() + 5
    pipe = conn.pipeline()

    while time.time() < end:
        try:
            pipe.watch("market:")

            # 判断商品是否已经出售
            price = pipe.zscore("market:", item)
            if not price:
                pipe.unwatch()
                return None

            pipe.multi()
            # 下架
            pipe.zrem("market:", item)
            # 返回到玩家包裹中
            pipe.sadd(inventory, item_id)
            pipe.execute()
            return True
        except redis.exceptions.WatchError:
            pass

    return False
