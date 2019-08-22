#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os


# 持久化选项
# 第一种：快照（snapshotting）
# 它可以将存在于某一时刻的所有数据都写入硬盘里面
# 相关配置
# save 60 10000  多久执行一次快照
# stop-writes-on-bgsave-error no    创建快照失败后，是否仍然继续
# rdbcompression yes    是否压缩
# dbfilename dump.rdb   快照文件的名字

# 创建快照的方式
# 1.客户端可以通过向Redis发送BGSAVE命令创建快照（windows系统除外）
# Redis会调用fork来创建一个子进程，将快照写入硬盘
# 2.客户端向Redis发送SAVE命令创建一个快照，Redis服务器阻塞，并执行创建快照的命令
# 通常我们会使用BGSAVE命令，除非内存不够或者可以接受的等待
# 3.设置save 60 10000
# 则从Redis最近一次创建快照算起，当60秒内有10000次写入操作时，Redis自动执行BGSAVE命令
# save配置可以设置多个，各条件之间采取or策略
# 4.Redis收到shutdown、term信号时，会执行一次save命令
# 并在执行完毕后，关闭服务器
# 5.当一个 Redis 服务器连接另一个 Redis 服务器，并向对方发送 SYNC 命令来开始一次复
# 制操作的时候，如果主服务器目前没有在执行 BGSAVE 操作，或者主服务器并非刚刚执
# 行完 BGSAVE 操作，那么主服务器就会执行 BGSAVE 命令


# process_logs()函数会将被处理日志的信息存储到 Redis 里面
def process_logs(conn, path, callback):
    # 读取当前文件的处理进度
    current_file, offset = conn.mget("progress:file", "progress:position")
    pipe = conn.pipeline()

    def update_progress():
        pipe.mset({"progress:file": current_file, "progress:position": offset})
        pipe.excute()

    for fname in sorted(os.listdir(path)):
        if fname < current_file:
            continue
        inp = open(os.path.join(path, fname), "rb")
        if fname == current_file:
            inp.seek(int(offset, 10))
        else:
            offset = 0

        current_file = None

        for lno, line in enumerate(inp):
            callback(pipe, line)
            offset += int(offset) + len(line)

            if not (lno + 1) % 1000:
                update_progress()
        update_progress()
        inp.close()

    # 第二种：只追加文件（append-only file，AOF）
    # 它会在执行写命令时，将被执行的写命令复制到硬盘里面
    # 相关配置
    # appendonly no 是否只是用aof持久化
    # appendfsync everysec  多久写一次磁盘
    # no-appendfsync-on-rewrite no  压缩时候是否执行同步操作
    # auto-aof-rewrite-percentage 100   多久执行一次aof压缩
    # auto-aof-rewrite-min-size 64mb
