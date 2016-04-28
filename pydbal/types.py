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

from pydbal.exception import DBALTypesError


class BaseType:
    __metaclass__ = ABCMeta

    ARRAY = "array"
    BOOLEAN = "boolean"
    SMALLINT = "smallint"
    INTEGER = "integer"
    BIGINT = "bigint"
    DECIMAL = "decimal"
    FLOAT = "float"
    STRING = "string"
    TEXT = "text"
    BINARY = "binary"
    BLOB = "blob"
    DATE = "date"
    TIME = "time"
    DATETIME = "datetime"
    GUID = "guid"

    def __init__(self):
        pass

    @staticmethod
    def get_type(name):
        if name not in BaseType.TYPES:
            raise DBALTypesError.unknown_type(name)
        return BaseType.TYPES[name]

    @staticmethod
    @abstractmethod
    def get_name():
        pass


class ArrayType(BaseType):
    @staticmethod
    def get_name():
        return BaseType.ARRAY


class BooleanType(BaseType):
    @staticmethod
    def get_name():
        return BaseType.BOOLEAN


class SmallIntType(BaseType):
    @staticmethod
    def get_name():
        return BaseType.SMALLINT


class IntegerType(BaseType):
    @staticmethod
    def get_name():
        return BaseType.INTEGER


class BigIntType(BaseType):
    @staticmethod
    def get_name():
        return BaseType.BIGINT


class DecimalType(BaseType):
    @staticmethod
    def get_name():
        return BaseType.DECIMAL


class FloatType(BaseType):
    @staticmethod
    def get_name():
        return BaseType.FLOAT


class StringType(BaseType):
    @staticmethod
    def get_name():
        return BaseType.STRING


class TextType(BaseType):
    @staticmethod
    def get_name():
        return BaseType.TEXT


class BinaryType(BaseType):
    @staticmethod
    def get_name():
        return BaseType.BINARY


class BlobType(BaseType):
    @staticmethod
    def get_name():
        return BaseType.BLOB


class DateType(BaseType):
    @staticmethod
    def get_name():
        return BaseType.DATE


class TimeType(BaseType):
    @staticmethod
    def get_name():
        return BaseType.TIME


class DateTimeType(BaseType):
    @staticmethod
    def get_name():
        return BaseType.DATETIME


class GuidType(BaseType):
    @staticmethod
    def get_name():
        return BaseType.GUID


BaseType.TYPES = {
    BaseType.ARRAY: ArrayType,
    BaseType.BOOLEAN: BooleanType,
    BaseType.SMALLINT: SmallIntType,
    BaseType.INTEGER: IntegerType,
    BaseType.BIGINT: BigIntType,
    BaseType.DECIMAL: DecimalType,
    BaseType.FLOAT: FloatType,
    BaseType.STRING: StringType,
    BaseType.TEXT: TextType,
    BaseType.BINARY: BinaryType,
    BaseType.BLOB: BlobType,
    BaseType.DATE: DateType,
    BaseType.TIME: TimeType,
    BaseType.DATETIME: DateTimeType,
    BaseType.GUID: GuidType
}
