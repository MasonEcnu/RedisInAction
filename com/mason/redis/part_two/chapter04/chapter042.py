#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# 复制replication
# slaveof host port
# 如果redis服务器启动时有这个配置
# 则该服务器将根据该选项给定的 IP 地址和端口号来连接主服务器

# # slaveof no one
# 对于一个正在运行的 Redis 服务器，用户可以通过发送 SLAVEOF no one 命令来让服务器终止复制
# 操作，不再接受主服务器的数据更新；也可以通过发送 SLAVEOF host port 命令来让服务器
# 开始复制一个新的主服务器。

# Redis复制的启动过程
# 步 骤  主服务器操作  从服务器操作
# 1
# （等待命令进入）
# 连接（或者重连接）主服务器，发送 SYNC 命令
# 2
# 开始执行 BGSAVE ，并使用缓冲区记录 BGSAVE
# 之后执行的所有写命令
# 根据配置选项来决定是继续使用现有的数据（如果有
# 的话）来处理客户端的命令请求，还是向发送请求的
# 客户端返回错误
# 3  BGSAVE 执行完毕，向从服务器发送快照文件，
# 并在发送期间继续使用缓冲区记录被执行的
# 写命令
# 丢弃所有旧数据（如果有的话），开始载入主服务器
# 发来的快照文件
# 4
# 快照文件发送完毕，开始向从服务器发送存储
# 在缓冲区里面的写命令
# 完成对快照文件的解释操作，像往常一样开始接受
# 命令请求
# 5
# 缓冲区存储的写命令发送完毕；从现在开始，
# 每执行一个写命令，就向从服务器发送相同的
# 写命令
# 执行主服务器发来的所有存储在缓冲区里面的写命
# 令；并从现在开始，接收并执行主服务器传来的每个
# 写命令


# ！！！
# 从服务器在与主服务器进行初始连接时，数据库中原有的所有数
# 据都将丢失，并被替换成主服务器发来的数据
# Redis 不支持主主复制（master-master replication）


# 主从链
# 如果从服务器 X 拥有从服务器 Y，那么当从服务器 X 在执行表 4-2 中的步骤 4 时，它将断开与从
# 服务器 Y 的连接，导致从服务器 Y 需要重新连接并重新同步（resync）。
