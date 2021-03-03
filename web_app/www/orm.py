import logging

import aiomysql

logging.basicConfig(level=logging.INFO)

import asyncio, os, json, time
from datetime import datetime

from aiohttp import web


@asyncio.coroutine
def create_pool(loop, **kw):
    '''
    数据库连接池
    :param loop:
    :param kw:
    :return:
    '''
    logging.info('create database connection pool...')
    global __pool
    __pool = yield from aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )


@asyncio.coroutine
def select(sql, args, size=None):
    '''
    select
    :param sql:
    :param args:
    :param size:
    :return:
    '''
    # log(sql, args)
    global __pool
    with (yield from __pool_) as connect:
        cur = yield from connect.cursor(aiomysql.DIctCursor)
        yield from cur.execute(sql.replace('?', '%s'), args or ())