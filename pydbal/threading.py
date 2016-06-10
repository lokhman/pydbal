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

import time

from threading import RLock
from contextlib import contextmanager
from pydbal.connection import Connection as UnsafeConnection


class SafeConnection:
    POOL_MAX_SIZE = 4
    LOCK = RLock()

    def __init__(self, *args, **kwargs):
        """Initialises thread-safe database connection.

        :param _pool_max_size: connection pool capacity (defaults to POOL_MAX_SIZE)
        """
        self._args = args
        self._kwargs = kwargs

        self._pool = {}
        self._pool_size = 0
        self._pool_max_size = kwargs.pop("_pool_max_size", SafeConnection.POOL_MAX_SIZE)

        assert self._pool_max_size > 0

    def _get_connection(self):
        with SafeConnection.LOCK:
            for conn, locked in self._pool.iteritems():
                if not locked:
                    return conn

            if self._pool_size == self._pool_max_size:
                while True:
                    time.sleep(1)
                    for conn, locked in self._pool.iteritems():
                        if not locked:
                            return conn

            conn = UnsafeConnection(*self._args, **self._kwargs)
            self._pool[conn] = False
            self._pool_size += 1
            return conn

    def _lock(self, conn):
        self._pool[conn] = True

    def _unlock(self, conn):
        self._pool[conn] = False

    @contextmanager
    def locked(self):
        """Context generator for `with` statement, yields thread-safe connection.

        :return: thread-safe connection
        :rtype: pydbal.connection.Connection
        """
        conn = self._get_connection()
        try:
            self._lock(conn)
            yield conn
        finally:
            self._unlock(conn)

    def query(self, sql, *args, **kwargs):
        """Executes an SQL SELECT query and returns rows generator.

        :param sql: query to execute
        :param args: parameters iterable
        :param kwargs: parameters iterable
        :return: rows generator
        :rtype: generator
        """
        with self.locked() as conn:
            for row in conn.query(sql, *args, **kwargs):
                yield row

    def execute(self, sql, *args, **kwargs):
        """Executes an SQL INSERT/UPDATE/DELETE query with the given parameters and returns the number of affected rows.

        :param sql: statement to execute
        :param args: parameters iterable
        :param kwargs: parameters iterable
        :return: number of affected rows
        :rtype: int
        """
        with self.locked() as conn:
            return conn.execute(sql, *args, **kwargs)

    def fetch(self, sql, *args, **kwargs):
        """Executes an SQL SELECT query and returns the first row or `None`.

        :param sql: statement to execute
        :param args: parameters iterable
        :param kwargs: parameters iterable
        :return: the first row or `None`
        """
        with self.locked() as conn:
            return conn.query(sql, *args, **kwargs).fetch()

    def fetch_all(self, sql, *args, **kwargs):
        """Executes an SQL SELECT query and returns all selected rows.

        :param sql: statement to execute
        :param args: parameters iterable
        :param kwargs: parameters iterable
        :return: all selected rows
        :rtype: list
        """
        with self.locked() as conn:
            return conn.query(sql, *args, **kwargs).fetch_all()

    def fetch_column(self, sql, *args, **kwargs):
        """Executes an SQL SELECT query and returns the first column of the first row or `None`.

        :param sql: statement to execute
        :param args: parameters iterable
        :param kwargs: parameters iterable
        :return: the first row of the first column or `None`
        """
        with self.locked() as conn:
            return conn.query(sql, *args, **kwargs).fetch_column()

    def transaction(self, callback):
        """Executes a function in a thread-safe transaction.

        The function gets passed locked Connection instance as an (optional) parameter.

        If an exception occurs during execution of the function or transaction commit,
        the transaction is rolled back and the exception re-thrown.

        :param callback: the function to execute in a transaction
        :return: the value returned by the `callback`
        """
        with self.locked() as conn:
            return conn.transaction(callback)

    def insert(self, table, values):
        """Inserts a table row with specified data.

        :param table: the expression of the table to insert data into, quoted or unquoted
        :param values: a dictionary containing column-value pairs
        :return: last inserted ID
        """
        with self.locked() as conn:
            return conn.insert(table, values)

    def update(self, table, values, identifier):
        """Updates a table row with specified data by given identifier.

        :param table: the expression of the table to update quoted or unquoted
        :param values: a dictionary containing column-value pairs
        :param identifier: the update criteria; a dictionary containing column-value pairs
        :return: the number of affected rows
        :rtype: int
        """
        with self.locked() as conn:
            return conn.update(table, values, identifier)

    def delete(self, table, identifier):
        """Deletes a table row by given identifier.

        :param table: the expression of the table to update quoted or unquoted
        :param identifier: the delete criteria; a dictionary containing column-value pairs
        :return: the number of affected rows
        :rtype: int
        """
        with self.locked() as conn:
            return conn.delete(table, identifier)
