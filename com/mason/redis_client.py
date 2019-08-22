#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import redis

pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, password=123456)
redisClient = redis.Redis(connection_pool=pool)
