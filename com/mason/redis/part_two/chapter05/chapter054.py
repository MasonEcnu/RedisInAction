#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time

from redis import Redis

# 服务的发现与配置
# 在线配置（live configuration）
# 普遍可访问位置（commonly accessible location）

LAST_CHECKED = None
IS_UNDER_MAINTENANCE = False


def is_under_maintenance(conn: Redis):
    # 若想在函数内部对函数外的变量进行操作，就需要在函数内部声明其为global
    global LAST_CHECKED, IS_UNDER_MAINTENANCE
    if LAST_CHECKED < time.time() - 1:
        LAST_CHECKED = time.time()
        IS_UNDER_MAINTENANCE = bool(conn.get("is_under_maintenance"))
    return IS_UNDER_MAINTENANCE
