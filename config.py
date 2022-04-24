#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

# conn
host = '127.0.0.1'
port = 3306
user = 'root'
password = 'root'

# database
# 只解析目标db的sql，多个库用 , 隔开，如-d db1 db2。可选。默认为空
databases = 'sshagent',
# 只解析目标table的sql，多张表用 , 隔开，如-t tbl1 tbl2。可选。默认为空
tables = ''

# binlog
# 起始解析文件，只需文件名，无需全路径 。必须。
start_file = 'mysql-bin.000001'
# 终止解析文件。可选。默认为start-file同一个文件。若解析模式为stop-never，此选项失效。
stop_file = ''
# 起始解析时间，格式'%Y-%m-%d %H:%M:%S'。可选。默认不过滤。
start_time = '2022-04-22 17:50:00'
# 终止解析时间，格式'%Y-%m-%d %H:%M:%S'。可选。默认不过滤。
stop_time = '2022-4-22 19:00:00'
# 起始解析位置。可选。默认为start-file的起始位置。
start_position = ''
# 终止解析位置。可选。默认为stop-file的最末位置；若解析模式为stop-never，此选项失效
stop_position = ''
# 持续解析binlog。可选。默认False，同步至执行命令时最新的binlog位置
stop_never = False
# 只解析dml，忽略ddl。可选。默认False
only_dml = False
# 只解析指定类型，支持INSERT, UPDATE, DELETE。多个类型用 , 隔开，用了此参数但没填任何类型，则三者都不解析
sql_type = 'INSERT', 'UPDATE', 'DELETE'
# no-primary-key 对INSERT语句去除主键。可选。默认False
no_pk = False

# output
# 生成回滚SQL，可解析大文件，不受内存限制。可选。默认False。与stop-never或no-primary-key不能同时添加
flashback = True
# SQL文件输出路径，tables不为空时，每个table的SQL文件保存在该路径下的table的文件夹下
output_path = 'F:\\bjgcs3' + os.sep + tables
# 输出到控制台。默认False
output_console = False
