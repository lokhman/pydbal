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

import itertools

from pydbal.platforms import BasePlatform
from pydbal.types import BaseType


class MySQLPlatform(BasePlatform):
    _KEYWORDS = (
        "ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE", "BEFORE", "BETWEEN", "BIGINT", "BINARY",
        "BLOB", "BOTH", "BY", "CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK", "COLLATE", "COLUMN",
        "CONDITION", "CONNECTION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE", "CROSS", "CURRENT_DATE",
        "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE", "DATABASES", "DAY_HOUR",
        "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELAYED", "DELETE",
        "DESC", "DESCRIBE", "DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DOUBLE", "DROP", "DUAL", "EACH", "ELSE",
        "ELSEIF", "ENCLOSED", "ESCAPED", "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH", "FLOAT", "FLOAT4", "FLOAT8",
        "FOR", "FORCE", "FOREIGN", "FROM", "FULLTEXT", "GOTO", "GRANT", "GROUP", "HAVING", "HIGH_PRIORITY",
        "HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND", "IF", "IGNORE", "IN", "INDEX", "INFILE", "INNER", "INOUT",
        "INSENSITIVE", "INSERT", "INT", "INT1", "INT2", "INT3", "INT4", "INT8", "INTEGER", "INTERVAL", "INTO", "IS",
        "ITERATE", "JOIN", "KEY", "KEYS", "KILL", "LABEL", "LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT", "LINES", "LOAD",
        "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB", "LONGTEXT", "LOOP", "LOW_PRIORITY", "MATCH",
        "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "MODIFIES",
        "NATURAL", "NOT", "NO_WRITE_TO_BINLOG", "NULL", "NUMERIC", "ON", "OPTIMIZE", "OPTION", "OPTIONALLY", "OR",
        "ORDER", "OUT", "OUTER", "OUTFILE", "PRECISION", "PRIMARY", "PROCEDURE", "PURGE", "RAID0", "RANGE", "READ",
        "READS", "REAL", "REFERENCES", "REGEXP", "RELEASE", "RENAME", "REPEAT", "REPLACE", "REQUIRE", "RESTRICT",
        "RETURN", "REVOKE", "RIGHT", "RLIKE", "SCHEMA", "SCHEMAS", "SECOND_MICROSECOND", "SELECT", "SENSITIVE",
        "SEPARATOR", "SET", "SHOW", "SMALLINT", "SONAME", "SPATIAL", "SPECIFIC", "SQL", "SQLEXCEPTION", "SQLSTATE",
        "SQLWARNING", "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING", "STRAIGHT_JOIN",
        "TABLE", "TERMINATED", "THEN", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING", "TRIGGER", "TRUE", "UNDO",
        "UNION", "UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE", "USAGE", "USE", "USING", "UTC_DATE", "UTC_TIME",
        "UTC_TIMESTAMP", "VALUES", "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "WHEN", "WHERE", "WHILE", "WITH",
        "WRITE", "X509", "XOR", "YEAR_MONTH", "ZEROFILL"
    )
    _KEYWORDS57 = (
        "ACCESSIBLE", "ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE", "BEFORE", "BETWEEN", "BIGINT",
        "BINARY", "BLOB", "BOTH", "BY", "CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK", "COLLATE",
        "COLUMN", "CONDITION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE", "CROSS", "CURRENT_DATE", "CURRENT_TIME",
        "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND",
        "DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELAYED", "DELETE", "DESC", "DESCRIBE",
        "DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DOUBLE", "DROP", "DUAL", "EACH", "ELSE", "ELSEIF",
        "ENCLOSED", "ESCAPED", "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH", "FLOAT", "FLOAT4", "FLOAT8", "FOR",
        "FORCE", "FOREIGN", "FROM", "FULLTEXT", "GET", "GRANT", "GROUP", "HAVING", "HIGH_PRIORITY", "HOUR_MICROSECOND",
        "HOUR_MINUTE", "HOUR_SECOND", "IF", "IGNORE", "IN", "INDEX", "INFILE", "INNER", "INOUT", "INSENSITIVE",
        "INSERT", "INT", "INT1", "INT2", "INT3", "INT4", "INT8", "INTEGER", "INTERVAL", "INTO", "IO_AFTER_GTIDS",
        "IO_BEFORE_GTIDS", "IS", "ITERATE", "JOIN", "KEY", "KEYS", "KILL", "LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT",
        "LINEAR", "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB", "LONGTEXT", "LOOP",
        "LOW_PRIORITY", "MASTER_BIND", "MASTER_SSL_VERIFY_SERVER_CERT", "MATCH", "MAXVALUE", "MEDIUMBLOB", "MEDIUMINT",
        "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "MODIFIES", "NATURAL",
        "NO_WRITE_TO_BINLOG", "NONBLOCKING", "NOT", "NULL", "NUMERIC", "ON", "OPTIMIZE", "OPTION", "OPTIONALLY", "OR",
        "ORDER", "OUT", "OUTER", "OUTFILE", "PARTITION", "PRECISION", "PRIMARY", "PROCEDURE", "PURGE", "RANGE", "READ",
        "READ_WRITE", "READS", "REAL", "REFERENCES", "REGEXP", "RELEASE", "RENAME", "REPEAT", "REPLACE", "REQUIRE",
        "RESIGNAL", "RESTRICT", "RETURN", "REVOKE", "RIGHT", "RLIKE", "SCHEMA", "SCHEMAS", "SECOND_MICROSECOND",
        "SELECT", "SENSITIVE", "SEPARATOR", "SET", "SHOW", "SIGNAL", "SMALLINT", "SPATIAL", "SPECIFIC", "SQL",
        "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SSL",
        "STARTING", "STRAIGHT_JOIN", "TABLE", "TERMINATED", "THEN", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING",
        "TRIGGER", "TRUE", "UNDO", "UNION", "UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE", "USAGE", "USE", "USING",
        "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP", "VALUES", "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "WHEN",
        "WHERE", "WHILE", "WITH", "WRITE", "XOR", "YEAR_MONTH", "ZEROFILL"
    )

    _TYPE_MAPPINGS = {
        "tinyint": BaseType.BOOLEAN,
        "smallint": BaseType.SMALLINT,
        "mediumint": BaseType.INTEGER,
        "int": BaseType.INTEGER,
        "integer": BaseType.INTEGER,
        "bigint": BaseType.BIGINT,
        "tinytext": BaseType.TEXT,
        "mediumtext": BaseType.TEXT,
        "longtext": BaseType.TEXT,
        "text": BaseType.TEXT,
        "varchar": BaseType.STRING,
        "string": BaseType.STRING,
        "char": BaseType.STRING,
        "date": BaseType.DATE,
        "datetime": BaseType.DATETIME,
        "timestamp": BaseType.DATETIME,
        "time": BaseType.TIME,
        "float": BaseType.FLOAT,
        "double": BaseType.FLOAT,
        "real": BaseType.FLOAT,
        "decimal": BaseType.DECIMAL,
        "numeric": BaseType.DECIMAL,
        "year": BaseType.DATE,
        "longblob": BaseType.BLOB,
        "blob": BaseType.BLOB,
        "mediumblob": BaseType.BLOB,
        "tinyblob": BaseType.BLOB,
        "binary": BaseType.BINARY,
        "varbinary": BaseType.BINARY,
        "set": BaseType.ARRAY
    }

    LENGTH_LIMIT_TINYTEXT = 255
    LENGTH_LIMIT_TEXT = 65535
    LENGTH_LIMIT_MEDIUMTEXT = 16777215
    LENGTH_LIMIT_TINYBLOB = 255
    LENGTH_LIMIT_BLOB = 65535
    LENGTH_LIMIT_MEDIUMBLOB = 16777215

    def _get_keywords(self):
        if self._driver.get_server_version() >= (5, 7):
            return MySQLPlatform._KEYWORDS57
        return MySQLPlatform._KEYWORDS

    def _get_type_mappings(self):
        return MySQLPlatform._TYPE_MAPPINGS

    def get_identifier_quote_character(self):
        return "`"

    def _modify_limit_sql(self, sql, limit, offset):
        if limit is not None:
            sql += " LIMIT " + str(limit)
            if offset is not None:
                sql += " OFFSET " + str(offset)
        elif offset is not None:
            sql += " LIMIT 18446744073709551615 OFFSET " + str(offset)
        return sql

    def set_transaction_isolation(self, level):
        self._driver.execute_and_clear(
            "SET SESSION TRANSACTION ISOLATION LEVEL " + self._get_transaction_isolation_sql(level))

    def get_databases(self):
        for row in self._fetch("SHOW DATABASES"):
            yield row[0][1]  # {"Database": ...}

    def get_views(self, database=None):
        database = "DATABASE()" if database is None else "'" + database + "'"
        sql = "SELECT TABLE_NAME, VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = " + database + ""
        for row in self._fetch(sql):  # [{"TABLE_NAME": ..., "VIEW_DEFINITION": ...}, ...]
            yield row[0][1], row[1][1]

    def get_tables(self, database=None):
        for row in self._fetch("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'"):
            yield row[0][1]

    def get_table_columns(self, table, database=None):
        database = "DATABASE()" if database is None else "'" + database + "'"
        sql = "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA, COLUMN_COMMENT, COLLATION_NAME " \
              "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = " + database + " AND TABLE_NAME = '" + table + "'"

        for row in self._fetch(sql):
            row = dict(row)

            type_match = BasePlatform._re_table_column_type.match(row["COLUMN_TYPE"])
            length = type_match.group("length")

            options = {}

            type_ = type_match.group("type")
            if type_ in ("char", "binary"):
                options["fixed"] = True
            elif type_ in ("float", "double", "real", "numeric", "decimal") and length is not None:
                decimal = (length + ",0").split(",")
                options["precision"] = int(decimal[0])
                options["scale"] = int(decimal[1])
                length = None
            elif type_ == "tinytext":
                length = MySQLPlatform.LENGTH_LIMIT_TINYTEXT
            elif type_ == "text":
                length = MySQLPlatform.LENGTH_LIMIT_TEXT
            elif type_ == "mediumtext":
                length = MySQLPlatform.LENGTH_LIMIT_MEDIUMTEXT
            elif type_ == "tinyblob":
                length = MySQLPlatform.LENGTH_LIMIT_TINYBLOB
            elif type_ == "blob":
                length = MySQLPlatform.LENGTH_LIMIT_BLOB
            elif type_ == "mediumblob":
                length = MySQLPlatform.LENGTH_LIMIT_MEDIUMBLOB
            elif type_ in ("tinyint", "smallint", "mediumint", "int", "integer", "bigint", "year"):
                length = None

            if length:
                options["length"] = int(length)
            if "unsigned" in row["COLUMN_TYPE"]:
                options["unsigned"] = True
            if row["IS_NULLABLE"] == "YES":
                options["notnull"] = False
            if row["COLUMN_DEFAULT"] is not None:
                options["default"] = row["COLUMN_DEFAULT"]
            if "auto_increment" in row["EXTRA"]:
                options["autoincrement"] = True
            if row["COLLATION_NAME"] is not None:
                options["platform_options"] = {"collation": row["COLLATION_NAME"]}

            type_ = self.get_type_mapping(type_)
            if row["COLUMN_COMMENT"]:
                comment, c_type = BasePlatform.get_type_from_comment(row["COLUMN_COMMENT"])
                if comment:
                    options["comment"] = row["COLUMN_COMMENT"]
                if c_type:
                    type_ = c_type

            yield row["COLUMN_NAME"], type_, options

    def get_table_indexes(self, table, database=None):
        database = "DATABASE()" if database is None else "'" + database + "'"
        sql = "SELECT INDEX_NAME, COLUMN_NAME, INDEX_TYPE, NON_UNIQUE FROM INFORMATION_SCHEMA.STATISTICS " \
              "WHERE TABLE_SCHEMA = " + database + " AND TABLE_NAME = '" + table + "'"

        indexes = []
        for row in self._fetch(sql):
            row = dict(row)

            options = {}
            if not row["NON_UNIQUE"]:
                options["unique"] = True
            if row["INDEX_NAME"] == "PRIMARY":
                options["primary"] = True

            if "FULLTEXT" in row["INDEX_TYPE"]:
                options["flags"] = ("FULLTEXT", )
            elif "SPATIAL" in row["INDEX_TYPE"]:
                options["flags"] = ("SPATIAL", )

            indexes.append((row["INDEX_NAME"], row["COLUMN_NAME"], options))

        for group, generator in itertools.groupby(indexes, lambda x: (x[0], x[2])):
            yield group[0], tuple(x[1] for x in generator), group[1]

    def get_table_foreign_keys(self, table, database=None):
        database = "DATABASE()" if database is None else "'" + database + "'"
        sql = "SELECT DISTINCT k.CONSTRAINT_NAME, k.COLUMN_NAME, k.REFERENCED_TABLE_NAME, k.REFERENCED_COLUMN_NAME " \
              "/*!50116 , c.UPDATE_RULE, c.DELETE_RULE */ FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k " \
              "/*!50116 INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS c " \
              "ON c.CONSTRAINT_NAME = k.CONSTRAINT_NAME AND c.TABLE_NAME = '" + table + "' */ " \
              "WHERE k.TABLE_NAME = '" + table + "' AND k.TABLE_SCHEMA = " + database + " " \
              "/*!50116 AND c.CONSTRAINT_SCHEMA = " + database + " */ AND k.REFERENCED_COLUMN_NAME IS NOT NULL"

        foreign_keys = []
        for row in self._fetch(sql):
            row = dict(row)

            options = {}
            delete_rule = row.get("DELETE_RULE")
            if delete_rule not in (None, "RESTRICT"):
                options["on_delete"] = delete_rule
            update_rule = row.get("UPDATE_RULE")
            if update_rule not in (None, "RESTRICT"):
                options["on_update"] = update_rule

            foreign_keys.append((
                row["CONSTRAINT_NAME"],
                row["COLUMN_NAME"],
                row["REFERENCED_TABLE_NAME"],
                row["REFERENCED_COLUMN_NAME"],
                options
            ))

        for group, generator in itertools.groupby(foreign_keys, lambda x: (x[0], x[2], x[4])):
            gen1, gen2 = itertools.tee(generator)
            yield group[0], tuple(x[1] for x in gen1), group[1], tuple(x[3] for x in gen2), group[2]
