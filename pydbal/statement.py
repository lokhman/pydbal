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

from collections import namedtuple

from pydbal.exception import DBALStatementError


class Statement:
    OBJECT_NAME = "Object"

    FETCH_DEFAULT = 0
    FETCH_TUPLE = 1
    FETCH_LIST = 2
    FETCH_DICT = 3
    FETCH_OBJECT = 4
    FETCH_COLUMN = 5

    _re_params = re.compile(
        r"(\?|(?<!:):[a-zA-Z_][a-zA-Z0-9_]*)(?=(?:(?:\\.|[^'\"\\])*['\"](?:\\.|[^'\"\\])*['\"])*(?:\\.|[^'\"\\])*\Z)")

    def __init__(self, connection):
        self._connection = connection

    def __iter__(self):
        return self.iterate()

    def clear(self):
        self._connection.get_driver().clear()

    def iterate(self, fetch_mode=None, column_index=0):
        if fetch_mode is None:
            fetch_mode = self._connection.get_fetch_mode()

        for row in self._connection.get_driver().iterate():
            yield self._transform(row, fetch_mode, column_index)

    @staticmethod
    def _transform(row, fetch_mode=FETCH_DEFAULT, column_index=0):
        if fetch_mode == Statement.FETCH_DICT:
            return dict(row)

        if fetch_mode == Statement.FETCH_COLUMN:
            return row[column_index][1]

        values = tuple(x[1] for x in row)
        if fetch_mode == Statement.FETCH_TUPLE:
            return values
        elif fetch_mode == Statement.FETCH_LIST:
            return list(values)
        elif fetch_mode == Statement.FETCH_OBJECT:
            return namedtuple(Statement.OBJECT_NAME, [x[0] for x in row])._make(values)

        return row

    def execute(self, sql, *args, **kwargs):
        for i, arg in enumerate(args):
            kwargs[i] = arg
        params = []
        sql = Statement._re_params.sub(self._prepare(kwargs, params), sql)
        return self._connection.get_driver().execute(sql, *params)

    def _prepare(self, params, exec_params):
        def replace(match):
            key = match.group()
            if key == "?":
                key = replace._param_counter
                replace._param_counter += 1
            else:
                key = key.lstrip(":")

            if key not in params:
                if isinstance(key, int):
                    raise DBALStatementError.missing_positional_parameter(key, params)
                else:
                    raise DBALStatementError.missing_named_parameter(key, params)

            param = params[key]

            if isinstance(param, (list, tuple)):
                exec_params.extend(param)
                return ", ".join((replace._placeholder, ) * len(param))

            exec_params.append(param)
            return replace._placeholder

        replace._placeholder = self._connection.get_driver().get_placeholder()
        replace._param_counter = 0

        return replace

    def fetch(self, fetch_mode=None):
        try:
            return next(self.iterate(fetch_mode))
        except StopIteration:
            return None

    def fetch_all(self, fetch_mode=None, column_index=0):
        return list(self.iterate(fetch_mode, column_index))

    def fetch_column(self, column_index=0):
        try:
            return next(self.iterate(Statement.FETCH_COLUMN, column_index))
        except StopIteration:
            return None
