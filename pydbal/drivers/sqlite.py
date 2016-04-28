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

import sqlite3
import warnings

from pydbal.drivers import BaseDriver
from pydbal.exception import DBALDriverError, DBALNotImplementedWarning
from pydbal.platforms.sqlite import SQLitePlatform


class SQLiteDriver(BaseDriver):
    _cursor = None
    _error = None

    def __init__(self, database, timeout=5.0, **params):
        self._logger = params.pop("logger")
        self._platform = SQLitePlatform(self)

        auto_commit = params.pop("auto_commit")
        if auto_commit:
            params["isolation_level"] = None
        else:
            params["isolation_level"] = "EXCLUSIVE"

        self._params = dict(database=database, timeout=timeout, **params)

    def _get_server_version_info(self):
        return sqlite3.sqlite_version_info

    def get_database(self):
        return self._params["database"]

    @staticmethod
    def _row_factory(cursor, row):
        return tuple((cursor.description[i][0], value) for i, value in enumerate(row))

    def connect(self):
        self.close()
        try:
            self._conn = sqlite3.connect(**self._params)
            self._conn.row_factory = self._row_factory
        except Exception as ex:
            raise DBALDriverError.driver_exception(self, ex)

    def close(self):
        self.clear()
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def clear(self):
        if self._cursor is not None:
            self._cursor.close()
            self._cursor = None

    def error_code(self):
        warnings.warn("Python 'sqlite3' library does not expose error codes.", DBALNotImplementedWarning)
        return 1 if self._error else 0

    def error_info(self):
        return self._error

    def execute(self, sql, *params):
        try:
            self._log(sql, *params)
            self._error = None
            self._cursor = self._conn.execute(sql, params)
            return self.row_count()
        except Exception as ex:
            if isinstance(ex, sqlite3.OperationalError):
                self._error = ex.message
            raise DBALDriverError.execute_exception(self, ex, sql, params)

    def iterate(self):
        if self._cursor is None:
            raise StopIteration

        for row in self._cursor:
            yield row

        self.clear()

    def row_count(self):
        return getattr(self._cursor, "rowcount", 0)

    def last_insert_id(self, seq_name=None):
        return getattr(self._cursor, "lastrowid", None)

    def begin_transaction(self):
        self.execute_and_clear("BEGIN TRANSACTION")

    def commit(self):
        self._log("COMMIT")
        self._conn.commit()

    def rollback(self):
        self._log("ROLLBACK")
        self._conn.rollback()

    @staticmethod
    def get_placeholder():
        return "?"

    def escape_string(self, value):
        return "'" + value.replace("'", "''") + "'"

    def get_name(self):
        return "sqlite"
