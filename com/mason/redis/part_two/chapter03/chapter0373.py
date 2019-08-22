#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# Redis 键的过期时间
# 命令    示例和描述
# PERSIST  PERSIST key-name — 移除键的过期时间
# TTL
# TTL key-name — 查看给定键距离过期还有多少秒
# EXPIRE  EXPIRE key-name seconds — 让给定键在指定的秒数之后过期
# EXPIREAT  EXPIREAT key-name timestamp — 将给定键的过期时间设置为给定的 UNIX 时间戳
# PTTL
# PTTL key-name — 查看给定键距离过期时间还有多少毫秒，这个命令在 Redis 2.6 或以上版本可用
# PEXPIRE  PEXPIRE key-name milliseconds — 让给定键在指定的毫秒数之后过期，这个命令在 Redis 2.6
# 或以上版本可用
# PEXPIREAT  PEXPIREAT key-name timestamp-milliseconds — 将一个毫秒级精度的 UNIX 时间戳设置
# 为给定键的过期时间，这个命令在 Redis 2.6 或以上版本可用

THIRTY_DAYS = 30 * 86400


def check_token(conn, token):
    return conn.get('login:' + token)  # A


def update_token(conn, token, user, item=None):
    conn.setex('login:' + token, user, THIRTY_DAYS)  # B
    key = 'viewed:' + token
    if item:
        conn.lrem(key, item)
        conn.rpush(key, item)
        conn.ltrim(key, -25, -1)
        conn.zincrby('viewed:', item, -1)
    conn.expire(key, THIRTY_DAYS)  # C


def add_to_cart(conn, session, item, count):
    key = 'cart:' + session
    if count <= 0:
        conn.hrem(key, item)
    else:
        conn.hset(key, item, count)
    conn.expire(key, THIRTY_DAYS)
