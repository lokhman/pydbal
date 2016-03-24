#!/usr/bin/env python
#
# Copyright (c) 2016 Alexander Lokhman <alex.lokhman@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from __future__ import absolute_import, division, print_function, with_statement

import logging

from .drivers import BaseDriver
from .statement import Statement
from .builder import SQLBuilder, ExpressionBuilder
from .exception import DBALConnectionError


class Connection:
    DRIVERS = {
        "mysql": "pydbal.drivers.mysql.MySQLDriver"
    }

    FETCH_DEFAULT = Statement.FETCH_DEFAULT
    FETCH_TUPLE = Statement.FETCH_TUPLE
    FETCH_LIST = Statement.FETCH_LIST
    FETCH_DICT = Statement.FETCH_DICT
    FETCH_OBJECT = Statement.FETCH_OBJECT
    FETCH_COLUMN = Statement.FETCH_COLUMN

    TRANSACTION_READ_UNCOMMITTED = 1
    TRANSACTION_READ_COMMITTED = 2
    TRANSACTION_REPEATABLE_READ = 3
    TRANSACTION_SERIALIZABLE = 4

    def __init__(self, driver, auto_connect=True, auto_commit=True, sql_logger=None, **params):
        if not isinstance(sql_logger, logging.Logger):
            sql_logger = self._get_default_sql_logger()
        self._sql_logger = sql_logger

        params = dict(auto_commit=auto_commit, sql_logger=sql_logger, **params)
        if isinstance(driver, BaseDriver):
            self._driver = driver
        else:
            if driver not in Connection.DRIVERS:
                raise DBALConnectionError.unknown_driver(driver, Connection.DRIVERS.iterkeys())
            pkg, cls = Connection.DRIVERS[driver].rsplit(".", 1)
            module = __import__(pkg, fromlist=[cls])
            self._driver = getattr(module, cls)(**params)

        self._params = params

        self._expr = ExpressionBuilder(self)

        self._platform = self._driver.get_platform()
        self._default_fetch_mode = Connection.FETCH_DICT
        self._auto_connect = auto_connect
        self._auto_commit = auto_commit

        self._transaction_nesting_level = 0
        self._transaction_isolation_level = None
        self._nest_transactions_with_savepoints = False
        self._is_rollback_only = False

        if auto_connect:
            self.connect()

    def __del__(self):
        self.close()

    def get_platform(self):
        return self._platform

    def get_platform_version(self):
        self._ensure_connected()
        return self._driver.get_server_version()

    def get_platform_version_info(self):
        self._ensure_connected()
        return self._driver.get_server_version_info()

    def connect(self):
        self._driver.connect()

    def close(self):
        self._driver.close()

    def is_connected(self):
        return self._driver.is_connected()

    def _ensure_connected(self):
        if not self.is_connected():
            if not self._auto_connect:
                raise DBALConnectionError.connection_closed()
            self.connect()

    def sql_builder(self):
        return SQLBuilder(self)

    def expression_builder(self):
        return self._expr

    def get_sql_logger(self):
        return self._sql_logger

    @staticmethod
    def _get_default_sql_logger():
        sql_logger = logging.getLogger("pydbal")
        sql_logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter(
            "[%(levelname)1.1s %(asctime)s %(name)s:%(module)s:%(lineno)d] %(message)s",
            "%y%m%d %H:%M:%S"))
        sql_logger.addHandler(handler)
        return sql_logger

    def get_driver(self):
        return self._driver

    def get_default_fetch_mode(self):
        return self._default_fetch_mode

    def set_fetch_mode(self, fetch_mode):
        self._default_fetch_mode = fetch_mode

    def query(self, sql, params=None):
        self._ensure_connected()
        stmt = Statement(self)
        stmt.execute(sql, params)
        return stmt

    def execute(self, sql, params=None):
        self._ensure_connected()
        return Statement(self).execute(sql, params)

    def column_count(self):
        self._ensure_connected()
        return self._driver.column_count()

    def row_count(self):
        self._ensure_connected()
        return self._driver.row_count()

    def last_insert_id(self):
        self._ensure_connected()
        return self._driver.last_insert_id()

    def error_code(self):
        self._ensure_connected()
        return self._driver.error_code()

    def error_info(self):
        self._ensure_connected()
        return self._driver.error_info()

    def begin_transaction(self):
        self._ensure_connected()
        self._transaction_nesting_level += 1
        if self._transaction_nesting_level == 1:
            self._driver.begin_transaction()
        elif self._nest_transactions_with_savepoints:
            self.create_savepoint(self._get_nested_transaction_savepoint_name())

    def commit(self):
        if self._transaction_nesting_level == 0:
            raise DBALConnectionError.no_active_transaction()
        if self._is_rollback_only:
            raise DBALConnectionError.commit_failed_rollback_only()

        self._ensure_connected()
        if self._transaction_nesting_level == 1:
            self._driver.commit()
        elif self._nest_transactions_with_savepoints:
            self.release_savepoint(self._get_nested_transaction_savepoint_name())

        self._transaction_nesting_level -= 1

        if not self._auto_commit and self._transaction_nesting_level == 0:
            self.begin_transaction()

    def commit_all(self):
        while self._transaction_nesting_level != 0:
            if not self._auto_commit and self._transaction_nesting_level == 1:
                return self.commit()
            self.commit()

    def rollback(self):
        if self._transaction_nesting_level == 0:
            raise DBALConnectionError.no_active_transaction()

        self._ensure_connected()
        if self._transaction_nesting_level == 1:
            self._transaction_nesting_level = 0
            self._driver.rollback()
            self._is_rollback_only = False
            if not self._auto_commit:
                self.begin_transaction()
        elif self._nest_transactions_with_savepoints:
            self.rollback_savepoint(self._get_nested_transaction_savepoint_name())
            self._transaction_nesting_level -= 1
        else:
            self._is_rollback_only = True
            self._transaction_nesting_level -= 1

    def transaction(self, callback):
        self.begin_transaction()
        try:
            result = callback()
            self.commit()
            return result
        except Exception as ex:
            self.rollback()
            raise ex

    def is_auto_commit(self):
        return self._auto_commit

    def set_auto_commit(self, auto_commit):
        auto_commit = bool(auto_commit)
        if auto_commit == self._auto_commit:
            return None

        self._auto_commit = auto_commit

        if self.is_connected() and self._transaction_nesting_level != 0:
            self.commit_all()

    def is_transaction_active(self):
        return self._transaction_nesting_level > 0

    def set_rollback_only(self):
        if self._transaction_nesting_level == 0:
            raise DBALConnectionError.no_active_transaction()
        self._is_rollback_only = True

    def is_rollback_only(self):
        if self._transaction_nesting_level == 0:
            raise DBALConnectionError.no_active_transaction()
        return self._is_rollback_only

    def set_transaction_isolation(self, level):
        self._ensure_connected()
        self._transaction_isolation_level = level
        return self._driver.execute_and_clear(self._platform.get_set_transaction_isolation_sql(level))

    def get_transaction_isolation(self):
        if self._transaction_isolation_level is None:
            self._transaction_isolation_level = self._platform.get_default_transaction_isolation_level()
        return self._transaction_isolation_level

    def set_nest_transactions_with_savepoints(self, nest_transactions_with_savepoints):
        if self._transaction_nesting_level > 0:
            raise DBALConnectionError.may_not_alter_nested_transaction_with_savepoints_in_transaction()
        if not self._platform.supports_savepoints():
            raise DBALConnectionError.savepoints_not_supported()
        self._nest_transactions_with_savepoints = bool(nest_transactions_with_savepoints)

    def get_nest_transactions_with_savepoints(self):
        return self._nest_transactions_with_savepoints

    def _get_nested_transaction_savepoint_name(self):
        return "PYDBAL_SAVEPOINT_" + str(self._transaction_nesting_level)

    def create_savepoint(self, savepoint):
        if not self._platform.supports_savepoints():
            raise DBALConnectionError.savepoints_not_supported()
        self._ensure_connected()
        self._driver.execute_and_clear(self._platform.create_savepoint(savepoint))

    def release_savepoint(self, savepoint):
        if not self._platform.supports_savepoints():
            raise DBALConnectionError.savepoints_not_supported()
        if self._platform.supports_release_savepoints():
            self._ensure_connected()
            self._driver.execute_and_clear(self._platform.release_savepoint(savepoint))

    def rollback_savepoint(self, savepoint):
        if not self._platform.supports_savepoints():
            raise DBALConnectionError.savepoints_not_supported()
        self._ensure_connected()
        self._driver.execute_and_clear(self._platform.rollback_savepoint(savepoint))

    def insert(self, table, values):
        assert isinstance(values, dict)

        return self.sql_builder().insert(table).values(values).execute()

    def update(self, table, values, identifier):
        assert isinstance(values, dict)
        assert isinstance(identifier, dict)

        sb = self.sql_builder().update(table)
        for column, value in values.iteritems():
            sb.set(column, sb.create_positional_parameter(value))
        for column, value in identifier.iteritems():
            func = self._expr.in_ if isinstance(value, (list, tuple)) else self._expr.eq
            sb.and_where(func(column, sb.create_positional_parameter(value)))
        return sb.execute()

    def delete(self, table, identifier):
        assert isinstance(identifier, dict)

        sb = self.sql_builder().delete(table)
        for column, value in identifier.iteritems():
            func = self._expr.in_ if isinstance(value, (list, tuple)) else self._expr.eq
            sb.and_where(func(column, sb.create_positional_parameter(value)))
        return sb.execute()
