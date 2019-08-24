#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# 验证快照文件和AOF文件
# aof文件检查
# 给定--fix 参数，程序将对 AOF 文件进行修复
# redis-check-aof [--fix] <file.aof>
# 快照文件检查
# redis-check-dump <dump.rdb>


# 更换故障主服务器
# 首先向机器B发送一个SAVE命令，让它创建一个新的快照文件，接着将这个快照文件发送给机器C，
# 并在机器C上面启动Redis。最后，让机器B成为机器C的从服务器
