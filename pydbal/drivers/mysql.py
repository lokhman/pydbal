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

import MySQLdb
import MySQLdb.cursors

from pydbal.drivers import BaseDriver
from pydbal.exception import DBALDriverError
from pydbal.platforms.mysql import MySQLPlatform


class MySQLDriver(BaseDriver):
    _cursor = None

    def __init__(self, host, user=None, password=None, database=None, port=3306, timeout=0, charset="utf8",
                 timezone="SYSTEM", sql_mode="TRADITIONAL", **params):
        self._logger = params.pop("logger")
        self._platform = MySQLPlatform(self)

        self._params = dict(
            use_unicode=True, charset=charset, init_command=("SET time_zone = '%s'" % timezone),
            connect_timeout=timeout, sql_mode=sql_mode, autocommit=params.pop("auto_commit"),
            cursorclass=MySQLdb.cursors.SSCursor, **params)

        if user is not None:
            self._params["user"] = user
        if password is not None:
            self._params["passwd"] = password
        if database is not None:
            self._params["db"] = database

        if "/" in host:
            self._params["unix_socket"] = host
        else:
            self._params["host"] = host
            self._params["port"] = int(port)

    def _get_server_version_info(self):
        return getattr(self._conn, "_server_version", None)

    def get_database(self):
        if "db" in self._params:
            return self._params["db"]
        self.execute("SELECT DATABASE()")
        return next(self.iterate())[0][1]

    def connect(self):
        self.close()
        try:
            self._conn = MySQLdb.connect(**self._params)
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
        return self._conn.errno()

    def error_info(self):
        return self._conn.error()

    def execute(self, sql, *params):
        try:
            return self._execute(sql, params)
        except MySQLdb.DatabaseError as ex:
            raise DBALDriverError.execute_exception(self, ex, sql, params)

    def _execute(self, sql, params):
        """Execute statement with reconnecting by connection closed error codes.

        2006 (CR_SERVER_GONE_ERROR): MySQL server has gone away
        2013 (CR_SERVER_LOST): Lost connection to MySQL server during query
        2055 (CR_SERVER_LOST_EXTENDED): Lost connection to MySQL server at '%s', system error: %d
        """
        try:
            return self._execute_unsafe(sql, params)
        except MySQLdb.OperationalError as ex:
            if ex.args[0] in (2006, 2013, 2055):
                self._log("Connection with server is lost. Trying to reconnect.")
                self.connect()
                return self._execute_unsafe(sql, params)
            raise  # MySQLdb.OperationalError

    def _execute_unsafe(self, sql, params):
        self._log(sql, *params)
        self._cursor = self._conn.cursor()
        return self._cursor.execute(sql, params)

    def iterate(self):
        if self._cursor is None:
            raise StopIteration

        columns = [x[0] for x in self._cursor.description]
        for row in self._cursor:
            yield zip(columns, row)

        self.clear()

    def row_count(self):
        return self._conn.affected_rows()

    def last_insert_id(self, seq_name=None):
        return self._conn.insert_id() or None

    def begin_transaction(self):
        self.execute_and_clear("START TRANSACTION")

    def commit(self):
        self._log("COMMIT")
        self._conn.commit()

    def rollback(self):
        self._log("ROLLBACK")
        self._conn.rollback()

    def escape_string(self, value):
        return self._conn.literal(value)

    def get_name(self):
        return "mysql"
