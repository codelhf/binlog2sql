#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.binlog2sql import Binlog2sql
from src.binlog2sql_util import command_line_args
from config import *

if __name__ == '__main__':
    # 命令行方式
    # args = command_line_args(sys.argv[1:])
    # 数据库连接设置
    conn_setting = {'host': host, 'port': port, 'user': user, 'passwd': password, 'charset': 'utf8'}
    # 实例化
    binlog2sql = Binlog2sql(connection_settings=conn_setting,
                            start_file=start_file, stop_file=stop_file,
                            start_time=start_time, stop_time=stop_time,
                            start_pos=start_position, stop_pos=stop_position, stop_never=stop_never,
                            only_schemas=databases, only_tables=tables,
                            only_dml=only_dml, sql_type=sql_type, no_pk=no_pk,
                            flashback=flashback, output_path=output_path, output_console=output_console)
    binlog2sql.process_binlog()
