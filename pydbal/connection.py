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
        """Initialises database connection.

        :param driver: database driver
        :param auto_connect: set connection auto (re)connect
        :param auto_commit: set connection auto commit
        :param sql_logger: SQL logger
        :param params: database connection parameters
        """
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
        self._fetch_mode = Connection.FETCH_DICT
        self._auto_connect = auto_connect
        self._auto_commit = auto_commit

        self._transaction_nesting_level = 0
        self._transaction_isolation_level = None
        self._nest_transactions_with_savepoints = False
        self._is_rollback_only = False

        if auto_connect:
            self.connect()

    def __del__(self):
        """Closes connection on instance destroy."""
        self.close()

    @staticmethod
    def _get_default_sql_logger():
        """Returns default SQL logger.

        :return: logger instance
        :rtype: logging.Logger
        """
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter(
            "[%(levelname)1.1s %(asctime)s %(name)s] %(message)s",
            "%y%m%d %H:%M:%S"))

        logger = logging.getLogger("pydbal")
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        return logger

    def get_sql_logger(self):
        """Returns connection SQL logger.

        :return: logger instance
        :rtype: logging.Logger
        """
        return self._sql_logger

    def get_driver(self):
        """Returns the DBAL driver instance.

        :return: driver instance
        :rtype: pydbal.drivers.BaseDriver
        """
        return self._driver

    def get_platform(self):
        """Returns the DBAL platform instance.

        :return: platform instance
        :rtype: pydbal.platforms.BasePlatform
        """
        return self._platform

    def get_platform_version(self):
        """Returns connected platform version.

        :return: platform version
        :rtype: str
        """
        self._ensure_connected()
        return self._driver.get_server_version()

    def get_platform_version_info(self):
        """Returns connected platform version information.

        :return: platform version info
        :rtype: tuple
        """
        self._ensure_connected()
        return self._driver.get_server_version_info()

    def connect(self):
        """Opens database connection."""
        self._driver.connect()

    def close(self):
        """Closes database connection."""
        self._driver.close()

    def is_connected(self):
        """Checks whether an actual connection to the database is established.

        :return: `True` if connection is open, `False` otherwise
        :rtype: bool
        """
        return self._driver.is_connected()

    def _ensure_connected(self):
        """Ensures database connection is still open."""
        if not self.is_connected():
            if not self._auto_connect:
                raise DBALConnectionError.connection_closed()
            self.connect()

    def sql_builder(self):
        """Creates the new SQL builder.

        :return: new SQL builder
        :rtype: pydbal.builder.SQLBuilder
        """
        return SQLBuilder(self)

    def get_expression_builder(self):
        """Gets an expression builder.

        :return: expression builder
        :rtype: pydbal.builder.ExpressionBuilder
        """
        return self._expr

    def get_fetch_mode(self):
        """Returns connection fetch mode.

        :return: connection fetch mode
        :rtype: int
        """
        return self._fetch_mode

    def set_fetch_mode(self, fetch_mode):
        """Sets connection fetch mode.

        :param fetch_mode: one of `Connection.FETCH_*` constants
        """
        self._fetch_mode = fetch_mode

    def query(self, sql, params=None):
        """Executes an SQL statement, returning a result set as a Statement object.

        :param sql: query to execute
        :param params: parameters iterable
        :return: result set as a Statement object
        :rtype: pydbal.statement.Statement
        """
        self._ensure_connected()
        stmt = Statement(self)
        stmt.execute(sql, params)
        return stmt

    def execute(self, sql, params=None):
        """Executes an SQL INSERT/UPDATE/DELETE query with the given parameters and returns the number of affected rows.

        :param sql: statement to execute
        :param params: parameters iterable
        :return: number of affected rows
        :rtype: int
        """
        self._ensure_connected()
        return Statement(self).execute(sql, params)

    def column_count(self):
        """Returns the number of columns in the result set.

        :return: number of columns in the result set; if there is no result set, this method should return `0`
        :rtype: int
        """
        self._ensure_connected()
        return self._driver.column_count()

    def row_count(self):
        """Returns the number of rows affected by the last DELETE, INSERT, or UPDATE statement.

        If the last executed SQL statement was a SELECT statement, some databases may return the number of rows returned
        by that statement. However, this behaviour is not guaranteed for all databases and should not be relied on for
        portable applications.

        :return: number of rows
        :rtype: int
        """
        self._ensure_connected()
        return self._driver.row_count()

    def last_insert_id(self, seq_name=None):
        """Returns the ID of the last inserted row, or the last value from a sequence object,
        depending on the underlying driver.

        Note: This method may not return a meaningful or consistent result across different drivers, because the
        underlying database may not even support the notion of AUTO_INCREMENT/IDENTITY columns or sequences.

        :param seq_name: name of the sequence object from which the ID should be returned
        :return: representation of the last inserted ID
        """
        self._ensure_connected()
        return self._driver.last_insert_id(seq_name)

    def error_code(self):
        """Fetches the SQLSTATE associated with the last database operation.

        :return: the last error code
        :rtype: int
        """
        self._ensure_connected()
        return self._driver.error_code()

    def error_info(self):
        """Fetches extended error information associated with the last database operation.

        :return: the last error information
        """
        self._ensure_connected()
        return self._driver.error_info()

    def begin_transaction(self):
        """Starts a transaction by suspending auto-commit mode."""
        self._ensure_connected()
        self._transaction_nesting_level += 1
        if self._transaction_nesting_level == 1:
            self._driver.begin_transaction()
        elif self._nest_transactions_with_savepoints:
            self.create_savepoint(self._get_nested_transaction_savepoint_name())

    def commit(self):
        """Commits the current transaction."""
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
        """Commits all current nesting transactions."""
        while self._transaction_nesting_level != 0:
            if not self._auto_commit and self._transaction_nesting_level == 1:
                return self.commit()
            self.commit()

    def rollback(self):
        """Cancels any database changes done during the current transaction."""
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
        """Executes a function in a transaction.

        The function gets passed this Connection instance as an (optional) parameter.

        If an exception occurs during execution of the function or transaction commit,
        the transaction is rolled back and the exception re-thrown.

        :param callback: the function to execute in a transaction.
        :return: the value returned by the `callback`
        :raise: Exception
        """
        self.begin_transaction()
        try:
            result = callback(self)
            self.commit()
            return result
        except Exception as ex:
            self.rollback()
            raise ex

    def is_auto_commit(self):
        """Returns the current auto-commit mode for this connection.

        :return: `True` if auto-commit mode is currently enabled for this connection, `False` otherwise
        :rtype: bool
        """
        return self._auto_commit

    def set_auto_commit(self, auto_commit):
        """Sets auto-commit mode for this connection.

        If a connection is in auto-commit mode, then all its SQL statements will be executed and committed as individual
        transactions. Otherwise, its SQL statements are grouped into transactions that are terminated by a call to
        either the method commit or the method rollback. By default, new connections are in auto-commit mode.

        NOTE: If this method is called during a transaction and the auto-commit mode is changed, the transaction is
        committed. If this method is called and the auto-commit mode is not changed, the call is a no-op.

        :param auto_commit: `True` to enable auto-commit mode; `False` to disable it
        """
        auto_commit = bool(auto_commit)
        if auto_commit == self._auto_commit:
            return None

        self._auto_commit = auto_commit

        if self.is_connected() and self._transaction_nesting_level != 0:
            self.commit_all()

    def is_transaction_active(self):
        """Checks whether a transaction is currently active.

        :return: `True` if a transaction is currently active, `False` otherwise
        :rtype: bool
        """
        return self._transaction_nesting_level > 0

    def set_rollback_only(self):
        """Marks the current transaction so that the only possible outcome for the transaction to be rolled back."""
        if self._transaction_nesting_level == 0:
            raise DBALConnectionError.no_active_transaction()
        self._is_rollback_only = True

    def is_rollback_only(self):
        """Checks whether the current transaction is marked for rollback only.

        :return: `True` if a transaction is marked for rollback only, `False` otherwise
        :rtype: bool
        """
        if self._transaction_nesting_level == 0:
            raise DBALConnectionError.no_active_transaction()
        return self._is_rollback_only

    def set_transaction_isolation(self, level):
        """Sets the transaction isolation level.

        :param level: the level to set
        """
        self._ensure_connected()
        self._transaction_isolation_level = level
        self._driver.execute_and_clear(self._platform.get_set_transaction_isolation_sql(level))

    def get_transaction_isolation(self):
        """Returns the currently active transaction isolation level.

        :return: the current transaction isolation level
        :rtype: int
        """
        if self._transaction_isolation_level is None:
            self._transaction_isolation_level = self._platform.get_default_transaction_isolation_level()
        return self._transaction_isolation_level

    def set_nest_transactions_with_savepoints(self, nest_transactions_with_savepoints):
        """Sets if nested transactions should use savepoints.

        :param nest_transactions_with_savepoints: `True` or `False`
        """
        if self._transaction_nesting_level > 0:
            raise DBALConnectionError.may_not_alter_nested_transaction_with_savepoints_in_transaction()
        if not self._platform.supports_savepoints():
            raise DBALConnectionError.savepoints_not_supported()
        self._nest_transactions_with_savepoints = bool(nest_transactions_with_savepoints)

    def get_nest_transactions_with_savepoints(self):
        """Returns if nested transactions should use savepoints.

        :return: `True` if should use savepoints, `False` otherwise
        :rtype: bool
        """
        return self._nest_transactions_with_savepoints

    def _get_nested_transaction_savepoint_name(self):
        """Returns the savepoint name to use for nested transactions

        :return: a string with the savepoint name or false
        :rtype: str
        """
        return "PYDBAL_SAVEPOINT_" + str(self._transaction_nesting_level)

    def create_savepoint(self, savepoint):
        """Creates a new savepoint.

        :param savepoint: the name of the savepoint to create
        :raise: pydbal.exception.DBALConnectionError
        """
        if not self._platform.supports_savepoints():
            raise DBALConnectionError.savepoints_not_supported()
        self._ensure_connected()
        self._driver.execute_and_clear(self._platform.create_savepoint(savepoint))

    def release_savepoint(self, savepoint):
        """Releases the given savepoint.

        :param savepoint: the name of the savepoint to release
        :raise: pydbal.exception.DBALConnectionError
        """
        if not self._platform.supports_savepoints():
            raise DBALConnectionError.savepoints_not_supported()
        if self._platform.supports_release_savepoints():
            self._ensure_connected()
            self._driver.execute_and_clear(self._platform.release_savepoint(savepoint))

    def rollback_savepoint(self, savepoint):
        """Rolls back to the given savepoint.

        :param savepoint: the name of the savepoint to rollback to
        :raise: pydbal.exception.DBALConnectionError
        """
        if not self._platform.supports_savepoints():
            raise DBALConnectionError.savepoints_not_supported()
        self._ensure_connected()
        self._driver.execute_and_clear(self._platform.rollback_savepoint(savepoint))

    def insert(self, table, values):
        """Inserts a table row with specified data.

        :param table: the expression of the table to insert data into, quoted or unquoted
        :param values: a dictionary containing column-value pairs
        :return: last inserted ID
        """
        assert isinstance(values, dict)

        return self.sql_builder().insert(table).values(values).execute()

    def update(self, table, values, identifier):
        """Updates a table row with specified data by given identifier.

        :param table: the expression of the table to update quoted or unquoted
        :param values: a dictionary containing column-value pairs
        :param identifier: the update criteria; a dictionary containing column-value pairs
        :return: the number of affected rows
        :rtype: int
        """
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
        """Deletes a table row by given identifier.

        :param table: the expression of the table to update quoted or unquoted
        :param identifier: the delete criteria; a dictionary containing column-value pairs
        :return: the number of affected rows
        :rtype: int
        """
        assert isinstance(identifier, dict)

        sb = self.sql_builder().delete(table)
        for column, value in identifier.iteritems():
            func = self._expr.in_ if isinstance(value, (list, tuple)) else self._expr.eq
            sb.and_where(func(column, sb.create_positional_parameter(value)))
        return sb.execute()
