#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time

from redis import Redis


# 为每个应用程序组件分别配置一个 Redis 服务器


def set_config(conn: Redis, config_type, component, config):
    conn.set("config:%s:%s" % (config_type, component), json.dumps(config))


CONFIGS = {
    "config:redis:main": {"host": "localhost", "port": 6379, "decode_responses": "True", "password": 123456},
    "config:redis:logs": {"host": "localhost", "port": 6379, "decode_responses": "True", "password": 123456}
}
CHECKED = {"config:logs:main": 1566822940.0}


def get_config(conn, config_type, component, wait=1):
    key = "config:%s:%s" % (config_type, component)
    # 检查是否需要对这个组件的配置进行更新
    if CHECKED.get(key) is None:
        CHECKED[key] = time.time()
    if CHECKED.get(key) < time.time() - wait:
        # 记录最近一次的更新时间
        CHECKED[key] = time.time()
        # 取出redis中的配置
        config = json.loads(conn.get(key) or {})
        # 将潜在的Unicode关键字参数转为字符串
        config = dict((str(k), config[k]) for k in config)
        # 获取正在使用的配置
        old_config = CONFIGS.get(key)

        # 如果配置有变化，则更新
        if config != old_config:
            CONFIGS[key] = config
        return CONFIGS[key]
