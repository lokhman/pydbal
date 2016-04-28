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


class BaseDriver:
    __metaclass__ = ABCMeta

    _server_version_info = None

    _logger = None
    _platform = None
    _conn = None

    @abstractmethod
    def __init__(self, **params):
        pass

    def __del__(self):
        self.close()

    def _log(self, log, *params):
        logger = self.get_logger()
        if logger is not None:
            if params:
                log += " " + str(list(params))
            logger.debug(log)

    def get_logger(self):
        try:
            # 'getChild' property available in Python 2.7+
            return self._logger.getChild(self.get_name())
        except AttributeError:
            return self._logger

    def get_platform(self):
        return self._platform

    def get_server_version(self):
        info = self.get_server_version_info()
        if info is None:
            return None
        return ".".join(map(str, info))

    def get_server_version_info(self):
        if not self._server_version_info:
            self._server_version_info = self._get_server_version_info()
        return self._server_version_info

    @abstractmethod
    def _get_server_version_info(self):
        pass

    @abstractmethod
    def get_database(self):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def clear(self):
        pass

    def is_connected(self):
        return self._conn is not None

    @abstractmethod
    def error_code(self):
        pass

    @abstractmethod
    def error_info(self):
        pass

    @abstractmethod
    def execute(self, sql, *params):
        pass

    def execute_and_clear(self, sql, *params):
        self.execute(sql, *params)
        self.clear()

    @abstractmethod
    def iterate(self):
        pass

    @abstractmethod
    def row_count(self):
        pass

    @abstractmethod
    def last_insert_id(self, seq_name=None):
        pass

    @abstractmethod
    def begin_transaction(self):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def rollback(self):
        pass

    @staticmethod
    def get_placeholder():
        return "%s"

    @abstractmethod
    def escape_string(self, value):
        pass

    @abstractmethod
    def get_name(self):
        pass
