#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import datetime
import getpass
import os
import platform
import sys
from contextlib import contextmanager

from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)

if sys.version > '3':
    PY3PLUS = True
else:
    PY3PLUS = False


def create_file(filepath, filename):
    if not os.path.exists(filepath):
        os.makedirs(filepath)
    return os.path.join(filepath, filename)


def create_unique_file(filename):
    version = 0
    result_file = filename
    # if we have to try more than 1000 times, something is seriously wrong
    while os.path.exists(result_file) and version < 1000:
        result_file = filename + '.' + str(version)
        version += 1
    if version >= 1000:
        raise OSError('cannot create unique file %s.[0-1000]' % filename)
    return result_file


@contextmanager
def file_open(filename, mode):
    f = open(filename, mode)
    try:
        yield f
    finally:
        f.close()


@contextmanager
def file_temp_open(filename, mode):
    f = open(filename, mode)
    try:
        yield f
    finally:
        f.close()
        os.remove(filename)


def is_dml_event(event):
    if isinstance(event, WriteRowsEvent) \
            or isinstance(event, UpdateRowsEvent) \
            or isinstance(event, DeleteRowsEvent):
        return True
    else:
        return False


def event_type(event):
    t = None
    if isinstance(event, WriteRowsEvent):
        t = 'INSERT'
    elif isinstance(event, UpdateRowsEvent):
        t = 'UPDATE'
    elif isinstance(event, DeleteRowsEvent):
        t = 'DELETE'
    return t


def concat_sql_from_binlog_event(cursor, binlog_event, row=None, e_start_pos=None, flashback=False, no_pk=False):
    if flashback and no_pk:
        raise ValueError('only one of flashback or no_pk can be True')
    if not (isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent)
            or isinstance(binlog_event, DeleteRowsEvent) or isinstance(binlog_event, QueryEvent)):
        raise ValueError('binlog_event must be WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent or QueryEvent')

    sql = ''
    if isinstance(binlog_event, WriteRowsEvent) \
            or isinstance(binlog_event, UpdateRowsEvent) \
            or isinstance(binlog_event, DeleteRowsEvent):
        pattern = generate_sql_pattern(binlog_event, row=row, flashback=flashback, no_pk=no_pk)
        sql = cursor.mogrify(pattern['template'], pattern['values'])
        time = datetime.datetime.fromtimestamp(binlog_event.timestamp)
        sql += ' #start %s end %s time %s' % (e_start_pos, binlog_event.packet.log_pos, time)
    elif flashback is False and isinstance(binlog_event, QueryEvent) \
            and binlog_event.query != 'BEGIN' \
            and binlog_event.query != 'COMMIT':
        if binlog_event.schema:
            sql = 'USE {0};\n'.format(binlog_event.schema)
        sql += '{0};'.format(fix_object(binlog_event.query))

    return sql


def reversed_lines(fin):
    """Generate the lines of file in reverse order."""
    part = ''
    for block in reversed_blocks(fin):
        if PY3PLUS:
            try:
                # block = block.decode("utf-8")
                block = platform.system() == 'Windows' and block.decode("gbk") or block.decode("utf-8")
            except:
                continue
        for c in reversed(block):
            if c == '\n' and part:
                yield part[::-1]
                part = ''
            if c == '\r\n' and part:
                yield part[::-1]
                part = ''
            if type(c) != str:
                c = str(c)
            part += c
    if part:
        yield part[::-1]


# binlog2sql_util
def generate_sql_pattern(binlog_event, row=None, flashback=False, no_pk=False):
    template = ''
    values = []
    if flashback is True:
        if isinstance(binlog_event, WriteRowsEvent):
            template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table,
                ' AND '.join(map(compare_items, row['values'].items()))
            )
            values = map(fix_object, row['values'].values())
        elif isinstance(binlog_event, DeleteRowsEvent):
            template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                binlog_event.schema, binlog_event.table,
                ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                ', '.join(['%s'] * len(row['values']))
            )
            values = map(fix_object, row['values'].values())
        elif isinstance(binlog_event, UpdateRowsEvent):
            template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table,
                ', '.join(['`%s`=%%s' % x for x in row['before_values'].keys()]),
                ' AND '.join(map(compare_items, row['after_values'].items())))
            values = map(fix_object, list(row['before_values'].values()) + list(row['after_values'].values()))
    else:
        if isinstance(binlog_event, WriteRowsEvent):
            if no_pk:
                # print binlog_event.__dict__
                # tableInfo = (binlog_event.table_map)[binlog_event.table_id]
                # if tableInfo.primary_key:
                #     row['values'].pop(tableInfo.primary_key)
                if binlog_event.primary_key:
                    row['values'].pop(binlog_event.primary_key)

            template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                binlog_event.schema, binlog_event.table,
                ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                ', '.join(['%s'] * len(row['values']))
            )
            values = map(fix_object, row['values'].values())
        elif isinstance(binlog_event, DeleteRowsEvent):
            template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table, ' AND '.join(map(compare_items, row['values'].items())))
            values = map(fix_object, row['values'].values())
        elif isinstance(binlog_event, UpdateRowsEvent):
            template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table,
                ', '.join(['`%s`=%%s' % k for k in row['after_values'].keys()]),
                ' AND '.join(map(compare_items, row['before_values'].items()))
            )
            values = map(fix_object, list(row['after_values'].values()) + list(row['before_values'].values()))

    return {'template': template, 'values': list(values)}


def reversed_blocks(fin, block_size=4096):
    """Generate blocks of file's contents in reverse order."""
    fin.seek(0, os.SEEK_END)
    here = fin.tell()
    while 0 < here:
        delta = min(block_size, here)
        here -= delta
        fin.seek(here, os.SEEK_SET)
        yield fin.read(delta)


def compare_items(items):
    # caution: if v is NULL, may need to process
    (k, v) = items
    if v is None:
        return '`%s` IS %%s' % k
    else:
        return '`%s`=%%s' % k


def fix_object(value):
    """Fixes python objects so that they can be properly inserted into SQL queries"""
    if isinstance(value, set):
        value = ','.join(value)
    if PY3PLUS and isinstance(value, bytes):
        return value.decode('utf-8')
    elif not PY3PLUS and isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        return value


def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)
    if not args.start_file:
        raise ValueError('Lack of parameter: start_file')
    if args.flashback and args.stop_never:
        raise ValueError('Only one of flashback or stop-never can be True')
    if args.flashback and args.no_pk:
        raise ValueError('Only one of flashback or no_pk can be True')
    if (args.start_time and not is_valid_datetime(args.start_time)) or \
            (args.stop_time and not is_valid_datetime(args.stop_time)):
        raise ValueError('Incorrect datetime argument')
    if not args.password:
        args.password = getpass.getpass()
    else:
        args.password = args.password[0]
    return args


def parse_args():
    """parse args for binlog2sql"""
    parser = argparse.ArgumentParser(description='Parse MySQL binlog to SQL you want', add_help=False)
    parser.add_argument('--help', dest='help', action='store_true', default=False, help='help information')

    connect_setting = parser.add_argument_group('connect setting')
    connect_setting.add_argument('-h', '--host', dest='host', type=str, default='127.0.0.1',
                                 help='Host the MySQL database server located')
    connect_setting.add_argument('-P', '--port', dest='port', type=int, default=3306,
                                 help='MySQL port to use. default: 3306')
    connect_setting.add_argument('-u', '--user', dest='user', type=str, default='root',
                                 help='MySQL Username to log in as')
    connect_setting.add_argument('-p', '--password', dest='password', type=str, default='',
                                 help='MySQL Password to use')

    schema = parser.add_argument_group('schema filter')
    schema.add_argument('-d', '--databases', dest='databases', type=str, default='', nargs='*',
                        help='dbs you want to process')
    schema.add_argument('-t', '--tables', dest='tables', type=str, default='', nargs='*',
                        help='tables you want to process')

    binlog = parser.add_argument_group('binlog filter')
    binlog.add_argument('--start-file', dest='start_file', type=str,
                        help='Start binlog file to be parsed')
    binlog.add_argument('--stop-file', dest='stop_file', type=str, default='',
                        help="Stop binlog file to be parsed. default: '--start-file'")
    binlog.add_argument('--start-datetime', dest='start_time', type=str, default='',
                        help="Start time. format %%Y-%%m-%%d %%H:%%M:%%S")
    binlog.add_argument('--stop-datetime', dest='stop_time', type=str, default='',
                        help="Stop Time. format %%Y-%%m-%%d %%H:%%M:%%S;")
    binlog.add_argument('--start-position', dest='start_pos', type=int, default=4,
                        help='Start position of the --start-file')
    binlog.add_argument('--stop-position', dest='stop_pos', type=int, default=0,
                        help="Stop position. default: latest position of '--stop-file'")
    binlog.add_argument('--stop-never', dest='stop_never', type=str, default=False,
                        help="Continuously parse binlog. default: stop at the latest event when you start.")
    binlog.add_argument('--only-dml', dest='only_dml', type=str, default=False,
                        help='only print dml, ignore ddl. default: False')
    binlog.add_argument('--sql-type', dest='sql_type', type=str, default=['INSERT', 'UPDATE', 'DELETE'],
                        help='Sql type you want to process. default: INSERT, UPDATE, DELETE.')
    binlog.add_argument('--no-primary-key', dest='no_pk', type=bool, default=False,
                        help='Generate insert sql without primary key if exists. default: False')

    flashback = parser.add_argument_group('flashback filter')
    flashback.add_argument('-B', '--flashback', dest='flashback', type=bool, default=True,
                           help='Flashback data to start_position of start_file. default: True')
    flashback.add_argument('-O', '--output_path', dest='output_path', type=str, default=None,
                           help="Sql file output path.")
    flashback.add_argument('--output_console', dest='output_console', type=bool, default=False,
                           help="Show sql output console. default: False")
    return parser


def is_valid_datetime(string):
    try:
        datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
        return True
    except:
        return False
