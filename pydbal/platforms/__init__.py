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

from abc import ABCMeta, abstractmethod

from ..connection import Connection
from ..exception import DBALPlatformError


class BasePlatform:
    __metaclass__ = ABCMeta

    def __init__(self, driver):
        self._driver = driver

    def modify_limit_sql(self, sql, limit, offset=None):
        if limit is not None:
            limit = int(limit)
        if offset is not None:
            offset = int(offset)
            if offset < 0:
                raise DBALPlatformError.invalid_offset(offset)
            if offset > 0 and not self.supports_limit_offset():
                raise DBALPlatformError.offset_not_supported(self)
        return self._modify_limit_sql(sql, limit, offset)

    def _modify_limit_sql(self, sql, limit, offset):
        if limit is not None:
            sql += " LIMIT " + str(limit)
        if offset is not None:
            sql += " OFFSET " + str(offset)
        return sql

    def supports_limit_offset(self):
        return True

    def create_savepoint(self, savepoint):
        return "SAVEPOINT " + savepoint

    def release_savepoint(self, savepoint):
        return "RELEASE SAVEPOINT " + savepoint

    def rollback_savepoint(self, savepoint):
        return "ROLLBACK TO SAVEPOINT " + savepoint

    def supports_savepoints(self):
        return True

    def supports_release_savepoints(self):
        return self.supports_savepoints()

    def _get_set_transaction_isolation_sql(self, level):
        if level == Connection.TRANSACTION_READ_UNCOMMITTED:
            return "READ UNCOMMITTED"
        elif level == Connection.TRANSACTION_READ_COMMITTED:
            return "READ COMMITTED"
        elif level == Connection.TRANSACTION_REPEATABLE_READ:
            return "REPEATABLE READ"
        elif level == Connection.TRANSACTION_SERIALIZABLE:
            return "SERIALIZABLE"
        raise DBALPlatformError.invalid_isolation_level(level)

    def get_set_transaction_isolation_sql(self, level):
        raise DBALPlatformError.not_supported(self.get_set_transaction_isolation_sql)

    def get_default_transaction_isolation_level(self):
        return Connection.TRANSACTION_READ_COMMITTED
