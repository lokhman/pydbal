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
from binascii import crc32

from pydbal.types import BaseType
from pydbal.cache import cached


class BaseAsset:
    __metaclass__ = ABCMeta

    _name = None
    _namespace = None
    _quoted = False

    def __repr__(self):
        return "<%s.%s> %s" % (
            self.__class__.__module__,
            self.__class__.__name__,
            self.__str__()
        )

    @abstractmethod
    def __str__(self):
        pass

    def _set_name(self, name):
        if BaseAsset._is_identifier_quoted(name):
            self._quoted = True
            name = BaseAsset._trim_quotes(name)
        if "." in name:
            self._namespace, name = name.split(".", 1)
        self._name = name

    def get_name(self):
        if self._namespace:
            return self._namespace + "." + self._name
        return self._name

    def get_namespace(self):
        return self._namespace

    @staticmethod
    def _is_identifier_quoted(identifier):
        return identifier[:1] in ("`", '"', "[")

    @staticmethod
    def _trim_quotes(identifier):
        return identifier.encode("utf-8").translate(None, '`"[]')

    def _get_quoted_name(self, platform):
        keywords = platform.get_keywords()

        def quote(identifier):
            if self._quoted or identifier in keywords:
                return platform.quote_single_identifier(identifier)
            return identifier
        return ".".join(map(quote, self.get_name().split(".")))

    @staticmethod
    def _generate_identifier_name(column_names, prefix="", max_size=30):
        def encode(column_name):
            return "%X" % (crc32(column_name) & 0xffffffff)
        return (prefix.upper() + "_" + "".join(map(encode, column_names)))[:max_size]


class View(BaseAsset):
    def __init__(self, name, sql):
        self._set_name(name)
        self._sql = sql

    def __str__(self):
        return "'%s'" % self.get_name()

    def get_sql(self):
        return self._sql


class Table(BaseAsset):
    def __init__(self, name, columns=None, indexes=None, foreign_keys=None, **options):
        self._set_name(name)
        self._columns = set(columns)
        self._indexes = set(indexes)
        self._foreign_keys = set(foreign_keys)
        self._options = options

    def __str__(self):
        return "'%s'" % self.get_name()

    def get_columns(self):
        return tuple(self._columns)

    def get_indexes(self):
        return tuple(self._indexes)

    def get_foreign_keys(self):
        return tuple(self._foreign_keys)

    def get_options(self):
        return self._options.copy()


class Column(BaseAsset):
    def __init__(self, name, type_, length=None, precision=10, scale=0, unsigned=False, fixed=False, notnull=True,
                 default=None, autoincrement=False, column_definition=None, comment=None, platform_options=None,
                 **custom_schema_options):
        assert issubclass(type_, BaseType)

        self._set_name(name)
        self._type = type_

        if length is not None:
            length = int(length)
        self._length = length

        self._precision = int(precision)
        self._scale = int(scale)
        self._unsigned = bool(unsigned)
        self._fixed = bool(fixed)
        self._notnull = bool(notnull)
        self._default = default
        self._autoincrement = bool(autoincrement)

        self._column_definition = str(column_definition)
        self._comment = str(comment)

        self._platform_options = dict(platform_options) if platform_options else {}
        self._custom_schema_options = custom_schema_options

    def __str__(self):
        return "'%s' [%s]" % (self.get_name(), self._type.get_name())

    def get_type(self):
        return self._type

    def get_length(self):
        return self._length

    def get_precision(self):
        return self._precision

    def get_scale(self):
        return self._scale

    def is_unsigned(self):
        return self._unsigned

    def is_fixed(self):
        return self._fixed

    def is_notnull(self):
        return self._notnull

    def get_default(self):
        return self._default

    def is_autoincrement(self):
        return self._autoincrement

    def get_platform_options(self):
        return self._platform_options.copy()

    def get_column_definition(self):
        return self._column_definition

    def get_comment(self):
        return self._comment

    def get_custom_schema_options(self):
        return self._custom_schema_options.copy()


class Index(BaseAsset):
    def __init__(self, name, columns, unique=False, primary=False, flags=None, **options):
        self._set_name(name)
        self._columns = set(columns)

        self._unique = unique or primary
        self._primary = primary

        self._flags = set(flags or ())
        self._options = options

    def __str__(self):
        return "'%s' %s" % (self.get_name(), list(self._columns))

    def get_columns(self):
        return tuple(self._columns)

    def is_unique(self):
        return self._unique

    def is_primary(self):
        return self._primary

    def get_flags(self):
        return tuple(self._flags)

    def get_options(self):
        return self._options.copy()


class ForeignKey(BaseAsset):
    def __init__(self, name, local_columns, foreign_table, foreign_columns, **options):
        self._set_name(name)
        self._local_columns = set(local_columns)
        self._foreign_table = foreign_table
        self._foreign_columns = set(foreign_columns)
        self._options = options

    def __str__(self):
        return "`%s` %s -> '%s'.%s" % (
            self.get_name(),
            list(self._local_columns),
            self._foreign_table,
            list(self._foreign_columns)
        )

    def get_local_columns(self):
        return tuple(self._local_columns)

    def get_foreign_table(self):
        return self._foreign_table

    def get_foreign_columns(self):
        return tuple(self._foreign_columns)

    def get_options(self):
        return self._options.copy()


class SchemaManager:
    def __init__(self, connection):
        self._connection = connection
        self._platform = connection.get_platform()

    def __contains__(self, item):
        if isinstance(item, (list, tuple)):
            return all(x in self for x in item)
        try:
            name = item.get_name().lower()
        except AttributeError:
            name = item.lower()
        if isinstance(item, (Table, basestring)):
            return name in (x.lower() for x in self.get_table_names())
        elif isinstance(item, View):
            return name in (x.lower() for x in self.get_view_names())
        return False

    @cached
    def get_database_names(self):
        self._connection.ensure_connected()
        return list(self._platform.get_databases())

    @cached
    def get_views(self, database=None):
        self._connection.ensure_connected()
        return map(lambda x: View(*x), self._platform.get_views(database))

    def get_view_names(self, database=None, **kwargs):
        return map(View.get_name, self.get_views(database, **kwargs))

    @cached
    def get_table(self, table, database=None):
        columns = self.get_table_columns(table, database)
        indexes = self.get_table_indexes(table, database)

        foreign_keys = None
        if self._platform.is_foreign_keys_supported():
            foreign_keys = self.get_table_foreign_keys(table, database)

        return Table(table, columns, indexes, foreign_keys)

    @cached
    def get_table_names(self, database=None):
        self._connection.ensure_connected()
        return list(self._platform.get_tables(database))

    @cached
    def get_table_columns(self, table, database=None):
        self._connection.ensure_connected()

        def create_column(definition):
            return Column(definition[0], BaseType.get_type(definition[1]), **definition[2])

        return map(create_column, self._platform.get_table_columns(table, database))

    def get_table_column_names(self, table, database=None, **kwargs):
        return map(Column.get_name, self.get_table_columns(table, database, **kwargs))

    @cached
    def get_table_indexes(self, table, database=None):
        self._connection.ensure_connected()

        def create_index(definition):
            return Index(definition[0], definition[1], **definition[2])

        return map(create_index, self._platform.get_table_indexes(table, database))

    def get_table_index_names(self, table, database=None, **kwargs):
        return map(Index.get_name, self.get_table_indexes(table, database, **kwargs))

    @cached
    def get_table_foreign_keys(self, table, database=None):
        self._connection.ensure_connected()

        def create_foreign_key(definition):
            return ForeignKey(definition[0], definition[1], definition[2], definition[3], **definition[4])

        return map(create_foreign_key, self._platform.get_table_foreign_keys(table, database))

    def get_table_foreign_key_names(self, table, database=None, **kwargs):
        return map(ForeignKey.get_name, self.get_table_foreign_keys(table, database, **kwargs))
