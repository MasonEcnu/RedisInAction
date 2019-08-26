#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# 自动 Redis 连接管理
# 装饰器（decorator）
# 负责连接除配置服务器之外的所有其他 Redis 服务器

# 装饰器 Python 提供了一种语法，用于将函数 X 传入至另一个函数 Y 的内部，其中函数 Y 就被称
# 为装饰器。装饰器给用户提供了一个修改函数 X 行为的机会。有些装饰器可以用于校验参数，而有
# 些装饰器则可以用于注册回调函数，甚至还有一些装饰器可以用于管理连接
import functools

import redis

from com.mason.redis.part_two.chapter05.chapter0542 import CONFIGS, get_config

REDIS_CONNECTION = {}

config_connection = REDIS_CONNECTION.get("config:redis:main", object())


def redis_connection(component, wait=1):
    key = "config:redis:" + component

    def wrapper(function):
        @functools.wraps(function)
        def call(*args, **kwargs):
            old_config = CONFIGS.get(key, object())
            _config = get_config(config_connection, "redis", component, wait)

            config = {}
            if _config is None:
                config = old_config
                REDIS_CONNECTION[key] = redis.Redis(**config)
            else:
                for k, v in _config.iteritems():
                    config[k.encode("utf-8")] = v
            if config != old_config:
                REDIS_CONNECTION[key] = redis.Redis(**config)

            return function(REDIS_CONNECTION.get(key), *args, **kwargs)

        return call

    return wrapper


@redis_connection(component="logs", wait=1)
def log_recent(conn, app, message):
    print(conn)


log_recent("main", "user mason logged in")
