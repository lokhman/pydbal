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
import itertools

from pydbal.platforms import BasePlatform
from pydbal.cache import cached
from pydbal.types import BaseType
from pydbal.connection import Connection
from pydbal.exception import DBALPlatformError


class SQLitePlatform(BasePlatform):
    _KEYWORDS = (
        "ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ATTACH", "AUTOINCREMENT",
        "BEFORE", "BEGIN", "BETWEEN", "BY", "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT",
        "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATABASE",
        "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DROP", "EACH", "ELSE", "END",
        "ESCAPE", "EXCEPT", "EXCLUSIVE", "EXISTS", "EXPLAIN", "FAIL", "FOR", "FOREIGN", "FROM", "FULL", "GLOB", "GROUP",
        "HAVING", "IF", "IGNORE", "IMMEDIATE", "IN", "INDEX", "INDEXED", "INITIALLY", "INNER", "INSERT", "INSTEAD",
        "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "KEY", "LEFT", "LIKE", "LIMIT", "MATCH", "NATURAL", "NO", "NOT",
        "NOTNULL", "NULL", "OF", "OFFSET", "ON", "OR", "ORDER", "OUTER", "PLAN", "PRAGMA", "PRIMARY", "QUERY", "RAISE",
        "REFERENCES", "REGEXP", "REINDEX", "RELEASE", "RENAME", "REPLACE", "RESTRICT", "RIGHT", "ROLLBACK", "ROW",
        "SAVEPOINT", "SELECT", "SET", "TABLE", "TEMP", "TEMPORARY", "THEN", "TO", "TRANSACTION", "TRIGGER", "UNION",
        "UNIQUE", "UPDATE", "USING", "VACUUM", "VALUES", "VIEW", "VIRTUAL", "WHEN", "WHERE"
    )

    _TYPE_MAPPINGS = {
        "boolean": BaseType.BOOLEAN,
        "tinyint": BaseType.BOOLEAN,
        "smallint": BaseType.SMALLINT,
        "mediumint": BaseType.INTEGER,
        "int": BaseType.INTEGER,
        "integer": BaseType.INTEGER,
        "serial": BaseType.INTEGER,
        "bigint": BaseType.BIGINT,
        "bigserial": BaseType.BIGINT,
        "clob": BaseType.TEXT,
        "tinytext": BaseType.TEXT,
        "mediumtext": BaseType.TEXT,
        "longtext": BaseType.TEXT,
        "text": BaseType.TEXT,
        "varchar": BaseType.STRING,
        "longvarchar": BaseType.STRING,
        "varchar2": BaseType.STRING,
        "nvarchar": BaseType.STRING,
        "image": BaseType.STRING,
        "ntext": BaseType.STRING,
        "char": BaseType.STRING,
        "date": BaseType.DATE,
        "datetime": BaseType.DATETIME,
        "timestamp": BaseType.DATETIME,
        "time": BaseType.TIME,
        "float": BaseType.FLOAT,
        "double": BaseType.FLOAT,
        "double precision": BaseType.FLOAT,
        "real": BaseType.FLOAT,
        "decimal": BaseType.DECIMAL,
        "numeric": BaseType.DECIMAL,
        "binary": BaseType.BINARY,
        "blob": BaseType.BLOB
    }

    _re_foreign_key_details = re.compile(
        r"(?:CONSTRAINT\s+([^\s]+)\s+)?(?:FOREIGN\s+KEY[^\)]+\)\s*)?REFERENCES\s+[^\s]+\s+(?:\([^\)]+\))?"
        r"(?:[^,]*?(NOT\s+DEFERRABLE|DEFERRABLE)(?:\s+INITIALLY\s+(DEFERRED|IMMEDIATE))?)?", re.IGNORECASE)

    def _get_keywords(self):
        return SQLitePlatform._KEYWORDS

    def _get_type_mappings(self):
        return SQLitePlatform._TYPE_MAPPINGS

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

    def set_transaction_isolation(self, level):
        self._driver.execute_and_clear("PRAGMA read_uncommitted = " + self._get_transaction_isolation_sql(level))

    @staticmethod
    def _escape(name):
        return name.replace(".", "__")

    @cached
    def _fetch_table_create_sql(self, table):
        sql = "SELECT sql FROM (SELECT * FROM sqlite_master UNION ALL SELECT * FROM sqlite_temp_master) " \
              "WHERE type = 'table' AND name = '" + SQLitePlatform._escape(table) + "'"
        return dict(next(self._fetch(sql))).get("sql", "")

    def get_views(self, database=None):
        for row in self._fetch("SELECT name, sql FROM sqlite_master WHERE type = 'view' AND sql NOT NULL"):
            yield row[0][1], row[1][1]  # {"name": ..., "sql": ...}

    def get_tables(self, database=None):
        sql = "SELECT name FROM sqlite_master WHERE type = 'table' AND name != 'sqlite_sequence' " \
              "AND name != 'geometry_columns' AND name != 'spatial_ref_sys' UNION ALL " \
              "SELECT name FROM sqlite_temp_master WHERE type = 'table' ORDER BY name"
        for row in self._fetch(sql):  # [{"name": ...}, ...]
            yield row[0][1]

    def get_table_columns(self, table, database=None):
        create_sql = self._fetch_table_create_sql(table)
        for row in self._fetch("PRAGMA TABLE_INFO('" + SQLitePlatform._escape(table) + "')"):
            row = dict(row)

            type_match = BasePlatform._re_table_column_type.match(row["type"])
            length = type_match.group("length")

            options = {}

            type_ = type_match.group("type").lower()
            if type_ == "char":
                options["fixed"] = True
            elif type_ in ("float", "double", "real", "decimal", "numeric") and length is not None:
                decimal = (length + ",0").split(",")
                options["precision"] = int(decimal[0])
                options["scale"] = int(decimal[1])
                length = None
            elif type_ == "integer" and row["pk"]:
                options["autoincrement"] = True

            if length:
                options["length"] = int(length)
            if "unsigned" in row["type"]:
                options["unsigned"] = True
            if not row["notnull"]:
                options["notnull"] = False
            if row["dflt_value"] not in (None, "NULL"):
                options["default"] = row["dflt_value"]

            name = row["name"].replace("'", "''")
            type_ = self.get_type_mapping(type_)
            if type_ in (BaseType.STRING, BaseType.TEXT):
                re_ = r"(?:" + name + r")[^,(]+(?:\([^()]+\)[^,]*)?(?:(?:DEFAULT|CHECK)\s*(?:\(.*?\))?[^,]*)*" \
                      r"COLLATE\s+[\"']?([^\s,\"')]+)"
                matches = re.findall(re_, create_sql, re.IGNORECASE | re.DOTALL)
                options["platform_options"] = {"collation": matches[0] if matches else "BINARY"}

            re_ = r"[\s(,](?:" + name + r")(?:\(.*?\)|[^,(])*?,?((?:\s*--[^\n]*\n?)+)"
            matches = re.findall(re_, create_sql, re.IGNORECASE | re.DOTALL)
            if matches:
                comment = re.sub(r"^\s*--\s*", "", matches[0].rstrip("\r\n"), flags=re.MULTILINE)
                comment, c_type = BasePlatform.get_type_from_comment(comment)
                if comment:
                    options["comment"] = comment
                if c_type:
                    type_ = c_type

            yield row["name"], type_, options

    def get_table_indexes(self, table, database=None):
        table = SQLitePlatform._escape(table)

        primary = []
        for row in self._fetch("PRAGMA TABLE_INFO('" + table + "')"):
            row = dict(row)
            if row.get("pk"):
                primary.append(row["name"])

        if primary:
            yield "PRIMARY", primary, {
                "primary": True,
                "unique": True
            }

        for row in self._fetch("PRAGMA INDEX_LIST('" + table + "')"):
            row = dict(row)

            name = row["name"]
            if name.startswith("sqlite_"):
                continue

            options = {}
            if row["unique"]:
                options["unique"] = True

            columns = self._fetch("PRAGMA INDEX_INFO('" + SQLitePlatform._escape(name) + "')")
            yield name, [dict(column)["name"] for column in columns], options

    def get_table_foreign_keys(self, table, database=None):
        foreign_keys = []
        for row in self._fetch("PRAGMA FOREIGN_KEY_LIST('" + SQLitePlatform._escape(table) + "')"):
            row = dict(row)

            options = {}
            delete_rule = row.get("on_delete")
            if delete_rule not in (None, "RESTRICT"):
                options["on_delete"] = delete_rule
            update_rule = row.get("on_update")
            if update_rule not in (None, "RESTRICT"):
                options["on_update"] = update_rule

            foreign_keys.append([
                row["id"],
                row["from"],
                row["table"],
                row["to"],
                options
            ])

        if foreign_keys:
            create_sql = self._fetch_table_create_sql(table)
            matches = SQLitePlatform._re_foreign_key_details.findall(create_sql)
            for i, details in enumerate(matches):
                for foreign_key in foreign_keys:
                    if foreign_key[0] != i:
                        continue
                    if details[1].upper() == "DEFERRABLE":
                        foreign_key[4]["deferrable"] = True
                    if details[2].upper() == "DEFERRED":
                        foreign_key[4]["deferred"] = True
                    foreign_key[0] = details[0]

        for group, generator in itertools.groupby(foreign_keys, lambda x: (x[0], x[2], x[4])):
            gen1, gen2 = itertools.tee(generator)
            yield group[0], tuple(x[1] for x in gen1), group[1], tuple(x[3] for x in gen2), group[2]
