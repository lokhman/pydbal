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

import copy

from pydbal.exception import DBALBuilderError


class SQLBuilder:
    SELECT = 0
    DELETE = 1
    UPDATE = 2
    INSERT = 3

    STATE_DIRTY = 0
    STATE_CLEAN = 1

    def __init__(self, connection):
        self._connection = connection
        self._sql_parts = {
            "select":   [],
            "from":     [],
            "join":     {},
            "set":      [],
            "where":    None,
            "group_by": [],
            "having":   None,
            "order_by": [],
            "values":   {}
        }
        self._sql = None
        self._params = {}
        self._type = SQLBuilder.SELECT
        self._state = SQLBuilder.STATE_CLEAN
        self._first_result = None
        self._max_results = None
        self._param_counter = 0

    def __str__(self):
        return self.get_sql()

    def copy(self):
        return copy.copy(self)

    def expr(self):
        return self._connection.get_expression_builder()

    def get_type(self):
        return self._type

    def get_connection(self):
        return self._connection

    def set_parameter(self, key, value):
        if isinstance(key, str):
            key = key.lstrip(":")
        elif not isinstance(key, int):
            raise ValueError("Argument 'key' must be int or string.")
        self._params[key] = value
        return self

    def set_parameters(self, params):
        self._params.clear()
        if isinstance(params, dict):
            for key, param in params.iteritems():
                self.set_parameter(key, param)
        elif isinstance(params, (list, tuple)):
            for i, param in enumerate(params):
                self.set_parameter(i, param)
        else:
            raise ValueError("Argument 'params' must be dict, list or tuple.")
        return self

    def get_parameter(self, key):
        return self._params[key]

    def get_parameters(self):
        return self._params

    def set_first_result(self, first_result):
        self._state = SQLBuilder.STATE_DIRTY
        self._first_result = first_result
        return self

    def get_first_result(self):
        return self._first_result

    def set_max_results(self, max_results):
        self._state = SQLBuilder.STATE_DIRTY
        self._max_results = max_results
        return self

    def get_max_results(self):
        return self._max_results

    def _add(self, sql_part_name, sql_part, append=False):
        if not sql_part:
            return self

        self._state = SQLBuilder.STATE_DIRTY

        if not append:
            self.reset_sql_part(sql_part_name)

        if sql_part_name in ("select", "group_by"):
            self._sql_parts[sql_part_name].extend(sql_part)
        elif sql_part_name in ("from", "set", "order_by"):
            self._sql_parts[sql_part_name].append(sql_part)
        elif sql_part_name == "join":
            if sql_part[0] in self._sql_parts["join"]:
                self._sql_parts["join"][sql_part[0]].append(sql_part[1:])
            else:
                self._sql_parts["join"][sql_part[0]] = [sql_part[1:]]
        else:
            self._sql_parts[sql_part_name] = sql_part
        return self

    def select(self, select, *args):
        self._type = SQLBuilder.SELECT
        return self._add("select", (select,) + args)

    def add_select(self, select, *args):
        self._type = SQLBuilder.SELECT
        return self._add("select", (select,) + args, True)

    def from_(self, table, alias=None):
        return self._add("from", (table, alias), True)

    def insert(self, table):
        self._type = SQLBuilder.INSERT
        return self._add("from", (table,))

    def update(self, table, alias=None):
        self._type = SQLBuilder.UPDATE
        return self._add("from", (table, alias))

    def delete(self, table, alias=None):
        self._type = SQLBuilder.DELETE
        return self._add("from", (table, alias))

    def inner_join(self, from_alias, join, alias, *condition):
        condition = CompositeExpression(CompositeExpression.TYPE_AND, *condition)
        return self._add("join", (from_alias, "inner", join, alias, condition), True)

    def left_join(self, from_alias, join, alias, *condition):
        condition = CompositeExpression(CompositeExpression.TYPE_AND, *condition)
        return self._add("join", (from_alias, "left", join, alias, condition), True)

    def right_join(self, from_alias, join, alias, *condition):
        condition = CompositeExpression(CompositeExpression.TYPE_AND, *condition)
        return self._add("join", (from_alias, "right", join, alias, condition), True)

    join = inner_join

    def set(self, key, value):
        return self._add("set", key + " = " + value, True)

    def where(self, where, *args):
        return self._add("where", CompositeExpression(CompositeExpression.TYPE_AND, *(where,) + args))

    def and_where(self, where, *args):
        where = (where, ) + args
        expr = self._sql_parts["where"]
        if isinstance(expr, CompositeExpression):
            where = (str(expr), ) + where
        return self._add("where", CompositeExpression(CompositeExpression.TYPE_AND, *where))

    def or_where(self, where, *args):
        where = (where, ) + args
        expr = self._sql_parts["where"]
        if isinstance(expr, CompositeExpression):
            where = (str(expr), ) + where
        return self._add("where", CompositeExpression(CompositeExpression.TYPE_OR, *where))

    def group_by(self, group_by, *args):
        return self._add("group_by", (group_by,) + args)

    def add_group_by(self, group_by, *args):
        return self._add("group_by", (group_by,) + args, True)

    def set_value(self, column, value):
        self._sql_parts["values"][column] = value
        return self

    def values(self, values):
        if isinstance(values, dict):
            return self._add("values", values)
        return self

    def having(self, having, *args):
        return self._add("having", CompositeExpression(CompositeExpression.TYPE_AND, *(having,) + args))

    def and_having(self, having, *args):
        having = (having, ) + args
        expr = self._sql_parts["having"]
        if isinstance(expr, CompositeExpression):
            having = (str(expr), ) + having
        return self._add("having", CompositeExpression(CompositeExpression.TYPE_AND, *having))

    def or_having(self, having, *args):
        having = (having, ) + args
        expr = self._sql_parts["having"]
        if isinstance(expr, CompositeExpression):
            having = (str(expr), ) + having
        return self._add("having", CompositeExpression(CompositeExpression.TYPE_OR, *having))

    def order_by(self, sort, order="ASC"):
        return self._add("order_by", sort + " " + order.upper())

    def add_order_by(self, sort, order="ASC"):
        return self._add("order_by", sort + " " + order.upper(), True)

    def reset_sql_parts(self, sql_part_names=None):
        if sql_part_names is None:
            sql_part_names = self._sql_parts.keys()
        for sql_part_name in sql_part_names:
            self.reset_sql_part(sql_part_name)
        return self

    def reset_sql_part(self, sql_part_name):
        if isinstance(self._sql_parts[sql_part_name], list):
            del self._sql_parts[sql_part_name][:]
        elif isinstance(self._sql_parts[sql_part_name], dict):
            self._sql_parts[sql_part_name].clear()
        else:
            self._sql_parts[sql_part_name] = None
        self._state = SQLBuilder.STATE_DIRTY
        return self

    def create_named_parameter(self, value, placeholder=None):
        if placeholder is None:
            placeholder = ":pyValue" + str(self._param_counter)
            self._param_counter += 1
        self.set_parameter(placeholder[1:], value)
        return placeholder

    def create_positional_parameter(self, value):
        self.set_parameter(self._param_counter, value)
        self._param_counter += 1
        return "?"

    def get_sql(self):
        if self._sql is not None and self._state == SQLBuilder.STATE_CLEAN:
            return self._sql

        if self._type == SQLBuilder.INSERT:
            self._sql = self._get_sql_for_insert()
        elif self._type == SQLBuilder.DELETE:
            self._sql = self._get_sql_for_delete()
        elif self._type == SQLBuilder.UPDATE:
            self._sql = self._get_sql_for_update()
        else:
            self._sql = self._get_sql_for_select()

        self._state = SQLBuilder.STATE_CLEAN
        return self._sql

    def _get_sql_for_select(self):
        sql = "SELECT " + ", ".join(self._sql_parts["select"]) + " FROM "
        sql += ", ".join(self._get_from_clauses().itervalues())
        if self._sql_parts["where"] is not None:
            sql += " WHERE " + str(self._sql_parts["where"])
        if self._sql_parts["group_by"]:
            sql += " GROUP BY " + ", ".join(self._sql_parts["group_by"])
        if self._sql_parts["having"] is not None:
            sql += " HAVING " + str(self._sql_parts["having"])
        if self._sql_parts["order_by"]:
            sql += " ORDER BY " + ", ".join(self._sql_parts["order_by"])

        if self._max_results is not None or self._first_result is not None:
            return self._connection.get_platform().modify_limit_sql(sql, self._max_results, self._first_result)
        return sql

    def _get_from_clauses(self):
        from_clauses = {}
        known_aliases = set()
        for from_ in self._sql_parts["from"]:
            if from_[1] is None:
                table_sql = from_[0]
                table_reference = from_[0]
            else:
                table_sql = from_[0] + " " + from_[1]
                table_reference = from_[1]

            known_aliases.add(table_reference)
            from_clauses[table_reference] = table_sql + self._get_sql_for_joins(table_reference, known_aliases)

        for from_alias in self._sql_parts["join"].iterkeys():
            if from_alias not in known_aliases:
                raise DBALBuilderError.unknown_alias(from_alias, known_aliases)
        return from_clauses

    def _get_sql_for_joins(self, from_alias, known_aliases):
        sql = ""
        if from_alias in self._sql_parts["join"]:
            for join in self._sql_parts["join"][from_alias]:
                if join[2] in known_aliases:
                    raise DBALBuilderError.non_unique_alias(join[2], known_aliases)
                sql += " %s JOIN %s %s ON %s" % (
                    join[0].upper(),
                    join[1],
                    join[2],
                    str(join[3])
                )
                known_aliases.add(join[2])

            for join in self._sql_parts["join"][from_alias]:
                sql += self._get_sql_for_joins(join[2], known_aliases)
        return sql

    def _get_sql_for_insert(self):
        return "INSERT INTO %s (%s) VALUES(%s)" % (
            self._sql_parts["from"][0][0],
            ", ".join(self._sql_parts["values"].iterkeys()),
            ", ".join(self._sql_parts["values"].itervalues())
        )

    def _get_sql_for_update(self):
        sql = "UPDATE " + self._sql_parts["from"][0][0]
        if self._sql_parts["from"][0][1] is not None:
            sql += " " + self._sql_parts["from"][0][1]
        if self._sql_parts["set"]:
            sql += " SET " + ", ".join(self._sql_parts["set"])
        if self._sql_parts["where"] is not None:
            sql += " WHERE " + str(self._sql_parts["where"])
        return sql

    def _get_sql_for_delete(self):
        sql = "DELETE FROM " + self._sql_parts["from"][0][0]
        if self._sql_parts["from"][0][1] is not None:
            sql += " " + self._sql_parts["from"][0][1]
        if self._sql_parts["where"] is not None:
            sql += " WHERE " + str(self._sql_parts["where"])
        return sql

    def _prepare_params(self):
        args, kwargs = [], {}
        for key, value in self._params.iteritems():
            if isinstance(key, int):
                args.append(value)
            else:
                kwargs[key] = value
        return args, kwargs

    def execute(self):
        args, kwargs = self._prepare_params()
        if self._type == SQLBuilder.SELECT:
            return self._connection.query(self.get_sql(), *args, **kwargs)
        result = self._connection.execute(self.get_sql(), *args, **kwargs)
        if self._type == SQLBuilder.INSERT:
            return self._connection.last_insert_id()
        return result


class ExpressionBuilder:
    EQ = "="
    NEQ = "<>"
    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    LIKE = "LIKE"
    NOT_LIKE = "NOT LIKE"
    IN = "IN"
    NOT_IN = "NOT IN"

    def __init__(self, connection):
        self._connection = connection

    @staticmethod
    def and_x(*x):
        return CompositeExpression(CompositeExpression.TYPE_AND, *x)

    @staticmethod
    def or_x(*x):
        return CompositeExpression(CompositeExpression.TYPE_OR, *x)

    @staticmethod
    def comparison(x, operator, y):
        return x + " " + operator + " " + y

    @staticmethod
    def eq(x, y):
        return ExpressionBuilder.comparison(x, ExpressionBuilder.EQ, y)

    @staticmethod
    def neq(x, y):
        return ExpressionBuilder.comparison(x, ExpressionBuilder.NEQ, y)

    @staticmethod
    def lt(x, y):
        return ExpressionBuilder.comparison(x, ExpressionBuilder.LT, y)

    @staticmethod
    def lte(x, y):
        return ExpressionBuilder.comparison(x, ExpressionBuilder.LTE, y)

    @staticmethod
    def gt(x, y):
        return ExpressionBuilder.comparison(x, ExpressionBuilder.GT, y)

    @staticmethod
    def gte(x, y):
        return ExpressionBuilder.comparison(x, ExpressionBuilder.GTE, y)

    @staticmethod
    def is_null(x):
        return x + " " + ExpressionBuilder.IS_NULL

    @staticmethod
    def is_not_null(x):
        return x + " " + ExpressionBuilder.IS_NOT_NULL

    @staticmethod
    def like(x, y):
        return ExpressionBuilder.comparison(x, ExpressionBuilder.LIKE, y)

    @staticmethod
    def not_like(x, y):
        return ExpressionBuilder.comparison(x, ExpressionBuilder.NOT_LIKE, y)

    @staticmethod
    def in_(x, y):
        return ExpressionBuilder.comparison(x, ExpressionBuilder.IN, "(" + ", ".join(y) + ")")

    @staticmethod
    def not_in(x, y):
        return ExpressionBuilder.comparison(x, ExpressionBuilder.NOT_IN, "(" + ", ".join(y) + ")")

    def literal(self, value):
        return self._connection.get_driver().escape_string(value)


class CompositeExpression:
    TYPE_AND = "AND"
    TYPE_OR = "OR"

    def __init__(self, type_, *parts):
        self._type = type_
        self._parts = []
        self.add_multiple(parts)

    def __len__(self):
        return len(self._parts)

    def __str__(self):
        if len(self._parts) == 1:
            return CompositeExpression._str(self._parts[0])
        parts = map(CompositeExpression._str, self._parts)
        return "(" + (") " + self._type + " (").join(parts) + ")"

    def __iter__(self):
        return iter(self._parts)

    @staticmethod
    def _str(part):
        if isinstance(part, CompositeExpression):
            return str(part)
        return part

    def copy(self):
        return copy.copy(self)

    def get_type(self):
        return self._type

    def add_multiple(self, parts=None):
        if parts is None:
            parts = []
        for part in parts:
            self.add(part)
        return self

    def add(self, part):
        if (isinstance(part, CompositeExpression) and len(part) > 0) or part:
            self._parts.append(part)
        return self
