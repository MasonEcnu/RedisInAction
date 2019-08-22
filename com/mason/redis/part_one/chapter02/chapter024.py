#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time

from com.mason.redis.helper import Inventory


# 数据行缓存
# 程序使用了两个有序集合来记录应该在何时对缓存进行更新：第一个有序集合为调度
# （schedule）有序集合，它的成员为数据行的行 ID，而分值则是一个时间戳，这个时间戳记
# 录了应该在何时将指定的数据行缓存到 Redis 里面；第二个有序集合为延时（delay）有序
# 集合，它的成员也是数据行的行 ID，而分值则记录了指定数据行的缓存需要每隔多少秒更新一次。

# 为了让缓存函数定期地缓存数据行，程序首先需要将行 ID 和给定的延迟值添加到延迟有序
# 集合里面，然后再将行 ID 和当前时间的时间戳添加到调度有序集合里面。实际执行缓存操作的
# 函数需要用到数据行的延迟值，如果某个数据行的延迟值不存在，那么程序将取消对这个数据
# 行的调度。如果我们想要移除某个数据行已有的缓存，并且让缓存函数不再缓存那个数据行，
# 那么只需要把那个数据行的延迟值设置为小于或等于 0 就可以了。
def schedule_row_cache(conn, row_id, delay):
    conn.zadd("delay:", row_id, delay)
    conn.zadd("schedule:", row_id, time.time())


# 负责缓存数据行的函数会尝试读取调度有序集合的第一个元素以及该元素的分值，如果调度有序集合没有包含任何
# 元素，或者分值存储的时间戳所指定的时间尚未来临，那么函数会先休眠 50 毫秒，然后再重新
# 进行检查。当缓存函数发现一个需要立即进行更新的数据行时，缓存函数会检查这个数据行的延
# 迟值：如果数据行的延迟值小于或者等于 0，那么缓存函数会从延迟有序集合和调度有序集合里
# 面移除这个数据行的 ID，并从缓存里面删除这个数据行已有的缓存，然后再重新进行检查；对于
# 延迟值大于 0 的数据行来说，缓存函数会从数据库里面取出这些行，将它们编码为 JSON 格式并存
# 储到 Redis 里面，然后更新这些行的调度时间。

QUIT_FLAG = False


def cache_rows(conn):
    while not QUIT_FLAG:
        # 尝试获取下一个需要被缓存的数据行
        # 以及该行的调度时间戳，命令会返回一
        # 个包含零个或一个元组（tuple）的列表。
        next = conn.zrange("schedule:", 0, 0, withscores=True)
        now = time.time()
        if not next or next[0][1] > now:
            # 暂时没有行需要被缓存，
            # 休眠 50 毫秒后重试。
            time.sleep(0.05)
            continue
        row_id = next[0][0]

        # 提前获取下次调度的延迟时间
        delay = conn.zscore("delay:", row_id)
        if delay <= 0:
            # 此时，没必要再缓存了
            conn.zrem("delay:", row_id)
            conn.zrem("schedule:", row_id)
            conn.delete("inv:" + row_id)
            continue

        row = Inventory.get(row_id)
        conn.zadd("schedule:", row_id, now + delay)
        conn.set("inv:" + row_id, json.dumps(row.to_dict()))
