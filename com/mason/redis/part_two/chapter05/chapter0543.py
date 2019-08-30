#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# 自动 Redis 连接管理
# 装饰器（decorator）
# 负责连接除配置服务器之外的所有其他 Redis 服务器

# 装饰器 Python 提供了一种语法，用于将函数 X 传入至另一个函数 Y 的内部，其中函数 Y 就被称
# 为装饰器。装饰器给用户提供了一个修改函数 X 行为的机会。有些装饰器可以用于校验参数，而有
# 些装饰器则可以用于注册回调函数，甚至还有一些装饰器可以用于管理连接
import functools
import time

import redis

from com.mason.redis.part_two.chapter05.chapter0542 import CONFIGS, get_config

REDIS_CONNECTION = {}

config_connection = REDIS_CONNECTION.get("config:redis:main", object())


# 将应用组件的名字传给装饰器component
def redis_connection(component, wait=1):
    key = "config:redis:" + component

    # 包装器接受一个函数作为参数
    # 并使用另一个函数来包裹这个函数
    def wrapper(function):
        # 将被包裹函数的一些有用的元数据
        # 复制给配置处理器
        @functools.wraps(function)
        def call(*args, **kwargs):
            old_config = CONFIGS.get(key, object())
            # 如果旧配置存在，则去获取
            _config = get_config(config_connection, "redis", component, wait)

            config = {}
            if _config is None:
                config = old_config
                REDIS_CONNECTION[key] = redis.Redis(**config)
            else:
                for k, v in _config.iteritems():
                    config[k.encode("utf-8")] = v
            # 新老配置不同时，重新获取redis连接
            if config != old_config:
                REDIS_CONNECTION[key] = redis.Redis(**config)

            # 将 Redis 连接以及其他匹配的参数传递给被包裹函
            # 数，然后调用该函数并返回它的执行结果。

            # 同时使用*args, **kwargs，则函数可以接受任意参数传入
            return function(REDIS_CONNECTION.get(key), *args, **kwargs)

        return call

    return wrapper


# redis_connection()装饰器
@redis_connection(component="logs", wait=1)
def log_recent(conn, app, message):
    print(conn)


# log_recent("main", "user mason logged in")

# 装饰器模式
def metric(text):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            start = time.time() * 1000
            result = func(*args, **kw)
            end = time.time() * 1000
            print('%s() executed in %.2f ms' % (func.__name__, (end - start)))
            return result

        return wrapper

    return decorator


@metric("fast")
def fast(x, y):
    time.sleep(1)
    return x + y


print(fast(1, 2))
