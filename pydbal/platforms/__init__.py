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

import re

from abc import ABCMeta, abstractmethod

from pydbal.connection import Connection
from pydbal.exception import DBALPlatformError


class BasePlatform:
    __metaclass__ = ABCMeta

    _re_table_column_type = re.compile(r"^(?P<type>\w*)\s*(?:\(\s*(?P<length>\d+(?:,\d+)?)\s*\))?")
    _re_comment_type = re.compile(r"\s*\(DBALType:(?P<type>\w+)\)\s*")
    _keywords = None

    def __init__(self, driver):
        self._driver = driver

    def _fetch(self, sql):
        self._driver.execute(sql)
        return self._driver.iterate()

    def get_keywords(self):
        if self._keywords is None:
            self._keywords = self._get_keywords()
        return self._keywords

    @abstractmethod
    def _get_keywords(self):
        pass

    @abstractmethod
    def _get_type_mappings(self):
        pass

    def get_type_mapping(self, type_):
        mappings = self._get_type_mappings()
        if type_ not in mappings:
            raise DBALPlatformError.unknown_column_type(type_)
        return mappings[type_]

    @staticmethod
    def get_type_from_comment(comment, default=None):
        def replace(match):
            replace._type = match.group("type")
            return ""

        replace._type = default
        comment = BasePlatform._re_comment_type.sub(replace, comment)
        return comment, replace._type

    def quote_identifier(self, identifier):
        return ".".join(map(self.quote_single_identifier, identifier.split(".")))

    def quote_single_identifier(self, identifier):
        c = self.get_identifier_quote_character()
        return c + identifier.replace(c, c + c) + c

    def get_identifier_quote_character(self):
        return '"'

    def modify_limit_sql(self, sql, limit, offset=None):
        if limit is not None:
            limit = int(limit)
        if offset is not None:
            offset = int(offset)
            if offset < 0:
                raise DBALPlatformError.invalid_offset(offset)
            if offset > 0 and not self.is_limit_offset_supported():
                raise DBALPlatformError.offset_not_supported(self)
        return self._modify_limit_sql(sql, limit, offset)

    def _modify_limit_sql(self, sql, limit, offset):
        if limit is not None:
            sql += " LIMIT " + str(limit)
        if offset is not None:
            sql += " OFFSET " + str(offset)
        return sql

    @staticmethod
    def is_limit_offset_supported():
        return True

    @staticmethod
    def is_savepoints_supported():
        return True

    def is_release_savepoints_supported(self):
        return self.is_savepoints_supported()

    @staticmethod
    def is_foreign_keys_supported():
        return True

    def create_savepoint(self, savepoint):
        self._driver.execute_and_clear("SAVEPOINT " + savepoint)

    def release_savepoint(self, savepoint):
        self._driver.execute_and_clear("RELEASE SAVEPOINT " + savepoint)

    def rollback_savepoint(self, savepoint):
        self._driver.execute_and_clear("ROLLBACK TO SAVEPOINT " + savepoint)

    def _get_transaction_isolation_sql(self, level):
        if level == Connection.TRANSACTION_READ_UNCOMMITTED:
            return "READ UNCOMMITTED"
        elif level == Connection.TRANSACTION_READ_COMMITTED:
            return "READ COMMITTED"
        elif level == Connection.TRANSACTION_REPEATABLE_READ:
            return "REPEATABLE READ"
        elif level == Connection.TRANSACTION_SERIALIZABLE:
            return "SERIALIZABLE"
        raise DBALPlatformError.invalid_isolation_level(level)

    def set_transaction_isolation(self, level):
        raise DBALPlatformError.not_supported(self.set_transaction_isolation)

    @staticmethod
    def get_default_transaction_isolation_level():
        return Connection.TRANSACTION_READ_COMMITTED

    def get_databases(self):
        raise DBALPlatformError.not_supported(self.get_databases)

    def get_views(self, database=None):
        raise DBALPlatformError.not_supported(self.get_views)

    def get_tables(self, database=None):
        raise DBALPlatformError.not_supported(self.get_tables)

    def get_table_columns(self, table, database=None):
        raise DBALPlatformError.not_supported(self.get_table_columns)

    def get_table_indexes(self, table, database=None):
        raise DBALPlatformError.not_supported(self.get_table_indexes)

    def get_table_foreign_keys(self, table, database=None):
        raise DBALPlatformError.not_supported(self.get_table_foreign_keys)
