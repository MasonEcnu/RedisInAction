#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 接收日志文件
import time
import zlib

from redis import Redis

from com.mason.redis.part_two.chapter06.chapter065 import fetch_pending_messages


def process_logs_from_redis(conn: Redis, id, callback):
    while 1:
        # 获取文件列表
        f_data = fetch_pending_messages(conn, id)

        # 处理日志行
        for ch, m_data in f_data:
            for message in m_data:
                log_file = message["message"]
                if log_file == ":done":
                    return
                elif not log_file:
                    continue

                # 选择一个块读取器
                block_reader = read_blocks
                if log_file.endswith(".gz"):
                    block_reader = read_blocks_gz

                for line in read_lines(conn, ch + log_file, block_reader):
                    # 将日志行传给回调函数
                    callback(conn, line)

                # 强制刷新聚合数据缓存
                callback(conn, None)

                # 日志已经处理完毕
                # 向文件发送者报告
                conn.incr(ch + log_file + ":done")

        if not f_data:
            time.sleep(0.1)


def read_blocks(conn: Redis, key, block_size=2 ** 17):
    lb = block_size
    pos = 0
    # 尽可能多的读取数据
    # 直到读不到一个block_size为止
    while lb == block_size:
        # 获取数据块
        block = conn.substr(key, pos, pos + block_size - 1)
        yield block
        lb = len(block)
        pos += lb
    yield ""


def read_blocks_gz(conn: Redis, key):
    inp = b""
    decoder = None
    # 从redis里读取原始数据
    for block in read_blocks(conn, key):
        if not decoder:
            inp += block
            try:
                # 分析头信息
                # 以便取得被压缩的数据
                if inp[:3] != b"\x1f\x86\x08":
                    raise IOError("invalid gzip data")
                i = 10
                flag = inp[3]
                if flag & 4:
                    i += 2 + inp[i] + 256 * inp[i + 1]
                if flag & 8:
                    i = inp.index(b'\0', i) + 1
                if flag & 16:
                    i = inp.index(b'\0', i) + 1
                if flag & 2:
                    i += 2

                if i > len(inp):
                    # 程序读取的头信息并不完整
                    raise IndexError("not enough data")
            except (IndexError, ValueError):
                continue

            else:
                block = inp[i:]
                inp = None
                # 已经找到头信息
                # 准备好相应的解压程序
                decoder = zlib.decompressobj(-zlib.MAX_WBITS)
                if not block:
                    continue

        if not block:
            # 所有数据处理完毕
            # 向调用者返回最后剩下的数据块
            yield decoder.flush()

        yield decoder.decompress(block)


def read_lines(conn: Redis, key, read_blocks):
    out = b""
    for block in read_blocks(conn, key):
        if isinstance(block, str):
            block = block.encode()

        out += block
        # 查找位于文本最右端的换行符
        # 找不到就返回-1
        posn = out.rfind(b"\n")
        if posn >= 0:
            # 根据换行符来分割日志行
            for line in out[:posn].split(b"\n"):
                # 向调用者返回每个行
                yield line + b"\n"

            # 保留余下的数据
            out = out[posn + 1:]

        # 所有数据处理完毕
        if not block:
            yield out
            break
