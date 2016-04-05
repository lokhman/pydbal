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

from . import BasePlatform
from ..connection import Connection
from ..exception import DBALPlatformError


class SQLitePlatform(BasePlatform):

    def _modify_limit_sql(self, sql, limit, offset):
        if limit is None and offset is not None:
            return sql + " LIMIT -1 OFFSET " + str(offset)
        return super(SQLitePlatform, self)._modify_limit_sql(sql, limit, offset)

    def _get_transaction_isolation_sql(self, level):
        if level == Connection.TRANSACTION_READ_UNCOMMITTED:
            return 0
        elif level in (
            Connection.TRANSACTION_READ_COMMITTED,
            Connection.TRANSACTION_REPEATABLE_READ,
            Connection.TRANSACTION_SERIALIZABLE
        ):
            return 1

        raise DBALPlatformError.invalid_isolation_level(level)

    def get_set_transaction_isolation_sql(self, level):
        return "PRAGMA read_uncommitted = " + self._get_transaction_isolation_sql(level)
