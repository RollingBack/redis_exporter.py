#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 23/05/2017 11:40 AM
# @Author  : tianpeng.qi
# @Email    : lackgod@hotmail.com
# @File    : exporter.py
# @Software: PyCharm Community Edition
from redis import StrictRedis
from getopt import getopt, GetoptError
from sys import argv
from time import time


# 检查变量是否定义，用于设置默认值
def definded(x):
    return x in globals()


# 获取脚本变量
try:
    option_list, argvs = getopt(argv[1:], 'h:p:d:k:of:', ['db=', 'host=', 'port=', 'keys=', 'output-file='])
except GetoptError as error:
    print(error)
    exit(1)
# 解析脚本变量
for each_opt, value in option_list:
    if each_opt in ['-h', '--host']:
        host = value
    elif each_opt in ['-p', '--port']:
        port = value
    elif each_opt in ['-d', '--db']:
        db = value
    elif each_opt in ['-k', '--keys']:
        keys = value
    elif each_opt in ['-o', '--output']:
        output = True
    elif each_opt in ['-f', '--file']:
        txt_file = value
    else:
        pass

# 设置默认值
if not definded('host'):
    host = 'localhost'
if not definded('port'):
    port = 6379
if not definded('db'):
    db = 0
if not definded('output'):
    output = False
if not definded('txt_file'):
    txt_file = str(int(time()))
if not definded('keys'):
    print("没有给出要导出的键")
    exit(1)


# 解析redis协议
def gen_redis_proto(*args):
    args = map(str, args)
    proto = "*{0}\r\n".format(len(args))
    for each_arg in args:
        proto += "${0}\r\n{1}\r\n".format(len(each_arg), each_arg)
    return proto


# 获取redis的客户端
def get_redis_client(host='localhost', port=6379, db=0):
    return StrictRedis(host=host, port=port, db=db)

# 解析数据
client = get_redis_client(host, port, db)
keys = keys.split(',')
final_string = ''
for each_key in keys:
    key_type = client.type(each_key)
    if key_type == 'none':
        print('键{0}不存在'.format(each_key))
        if output:
            exit(1)
    elif key_type == 'string':
        value = client.get(each_key)
        final_string += gen_redis_proto('SET', each_key, value)
    elif key_type == 'hash':
        value = client.hgetall(each_key)
        for k in value:
            final_string += gen_redis_proto('HSET', each_key, k, value[k])
    elif key_type == 'list':
        value = client.lrange(each_key, 0, -1)
        for item in value:
            final_string += gen_redis_proto('RPUSH', each_key, item)
    elif key_type == 'set':
        value = client.smembers(each_key)
        for item in value:
            final_string += gen_redis_proto('SADD', each_key, item)
    elif key_type == 'zset':
        value = client.zrange(each_key, 0, -1, withscores=True)
        for item, score in value:
            final_string += gen_redis_proto('ZADD', each_key, score, item)
# 如果设置输出就输出，没有就生成一个文本文件
if output:
    print(final_string)
else:
    with open(txt_file, 'w') as f:
        f.write(final_string)
