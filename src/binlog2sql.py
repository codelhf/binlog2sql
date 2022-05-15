#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime

import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent

from .binlog2sql_util import (
    create_file, create_unique_file, file_open, file_temp_open, is_dml_event, event_type,
    concat_sql_from_binlog_event, reversed_lines
)


class Binlog2sql(object):

    def __init__(self, connection_settings, start_file=None, stop_file=None,
                 start_time=None, stop_time=None, start_pos=None, stop_pos=None, stop_never=False,
                 only_schemas=None, only_tables=None, only_dml=True, sql_type=None, no_pk=False,
                 flashback=False, output_path=None, output_console=None):
        """
        conn_setting: {'host': 127.0.0.1, 'port': 3306, 'user': user, 'passwd': passwd, 'charset': 'utf8'}
        """

        if not start_file:
            raise ValueError('Lack of parameter: start_file')

        self.conn_setting = connection_settings
        self.start_file = start_file
        self.stop_file = stop_file if stop_file else start_file
        if start_time:
            self.start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        else:
            self.start_time = datetime.datetime.strptime('1980-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
        if stop_time:
            self.stop_time = datetime.datetime.strptime(stop_time, "%Y-%m-%d %H:%M:%S")
        else:
            self.stop_time = datetime.datetime.strptime('2999-12-31 00:00:00', "%Y-%m-%d %H:%M:%S")
        self.start_pos = start_pos if start_pos else 4  # use binlog v4
        self.stop_pos = stop_pos
        self.stop_never = stop_never

        self.only_schemas = only_schemas if only_schemas else None
        self.only_tables = only_tables if only_tables else None

        self.only_dml = only_dml
        self.sql_type = [t.upper() for t in sql_type] if sql_type else []
        self.no_pk = no_pk

        self.flashback, self.output_path, self.output_console = (flashback, output_path, output_console)

        self.binlogList = []
        self.connection = pymysql.connect(**self.conn_setting)

        with self.connection as cursor:
            cursor.execute("SHOW MASTER STATUS")
            self.eof_file, self.eof_pos = cursor.fetchone()[:2]
            cursor.execute("SHOW MASTER LOGS")
            bin_index = [row[0] for row in cursor.fetchall()]
            if self.start_file not in bin_index:
                raise ValueError('parameter error: start_file %s not in mysql server' % self.start_file)
            binlog2i = lambda x: x.split('.')[1]
            for binary in bin_index:
                if binlog2i(self.start_file) <= binlog2i(binary) <= binlog2i(self.stop_file):
                    self.binlogList.append(binary)

            cursor.execute("SELECT @@server_id")
            self.server_id = cursor.fetchone()[0]
            if not self.server_id:
                raise ValueError('missing server_id in %s:%s' % (self.conn_setting['host'], self.conn_setting['port']))

    def process_binlog(self):
        stream = BinLogStreamReader(connection_settings=self.conn_setting,
                                    server_id=self.server_id, log_file=self.start_file, log_pos=self.start_pos,
                                    only_schemas=self.only_schemas, only_tables=self.only_tables,
                                    resume_stream=True, blocking=True)

        flag_last_event = False
        e_start_pos, last_pos = stream.log_pos, stream.log_pos

        origin_file = create_file(self.output_path, 'origin.sql')
        # to simplify code, we do not use flock for tmp_file.
        tmp_file = create_unique_file('%s.%s.txt' % (self.conn_setting['host'], self.conn_setting['port']))
        with file_open(origin_file, "w") as f_origin, file_temp_open(tmp_file, "w") as f_tmp, \
                self.connection as cursor:
            for binlog_event in stream:
                if not self.stop_never:
                    try:
                        event_time = datetime.datetime.fromtimestamp(binlog_event.timestamp)
                    except OSError:
                        event_time = datetime.datetime(1980, 1, 1, 0, 0)
                    if (stream.log_file == self.stop_file and stream.log_pos == self.stop_pos) or \
                            (stream.log_file == self.eof_file and stream.log_pos == self.eof_pos):
                        flag_last_event = True
                    elif event_time < self.start_time:
                        if not (isinstance(binlog_event, RotateEvent)
                                or isinstance(binlog_event, FormatDescriptionEvent)):
                            last_pos = binlog_event.packet.log_pos
                        continue
                    elif (stream.log_file not in self.binlogList) or \
                            (self.stop_pos and stream.log_file == self.stop_file and stream.log_pos > self.stop_pos) or \
                            (stream.log_file == self.eof_file and stream.log_pos > self.eof_pos) or \
                            (event_time >= self.stop_time):
                        break
                    # else:
                    #     raise ValueError('unknown binlog file or position')

                if isinstance(binlog_event, QueryEvent) and binlog_event.query == 'BEGIN':
                    e_start_pos = last_pos

                if isinstance(binlog_event, QueryEvent) and not self.only_dml:
                    sql = concat_sql_from_binlog_event(cursor=cursor, binlog_event=binlog_event,
                                                       flashback=self.flashback, no_pk=self.no_pk)
                    if sql:
                        f_origin.write(sql + '\n')
                        if self.output_console:
                            print(sql)
                elif is_dml_event(binlog_event) and event_type(binlog_event) in self.sql_type:
                    for row in binlog_event.rows:
                        sql = concat_sql_from_binlog_event(cursor=cursor, binlog_event=binlog_event, row=row,
                                                           flashback=self.flashback, no_pk=self.no_pk,
                                                           e_start_pos=e_start_pos)

                        f_origin.write(sql + '\n')
                        if self.output_console:
                            print(sql)
                        if self.flashback:
                            f_tmp.write(sql + '\n')

                if not (isinstance(binlog_event, RotateEvent) or isinstance(binlog_event, FormatDescriptionEvent)):
                    last_pos = binlog_event.packet.log_pos
                if flag_last_event:
                    break

            stream.close()
            f_origin.close()
            f_tmp.close()

            if self.flashback:
                rollback_file = create_file(self.output_path, 'rollback.sql')
                """print rollback sql from tmp_file"""
                if self.output_console:
                    print('###### rollback sql ######')
                with file_open(rollback_file, "w") as f_rollback, file_temp_open(tmp_file, "r") as f_tmp1:
                    # 从缓存文件读取原始SQL
                    for line in reversed_lines(f_tmp1):
                        f_rollback.write(line.rstrip() + '\n')
                        if self.output_console:
                            print(line.rstrip())
                    f_tmp1.close()
                f_rollback.close()

        return True

    def __del__(self):
        pass
