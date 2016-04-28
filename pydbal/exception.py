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


class DBALError(Exception):
    pass


class DBALConnectionError(DBALError):
    @classmethod
    def unknown_driver(cls, unknown_driver_name, known_driver_names):
        return cls(
            "The given driver '%s' is unknown, pyDBAL currently supports only the following drivers: %s." %
            (unknown_driver_name, ", ".join(sorted(known_driver_names))))

    @classmethod
    def connection_closed(cls):
        return cls("Connection with database is closed.")

    @classmethod
    def no_active_transaction(cls):
        return cls("There is no active transaction.")

    @classmethod
    def commit_failed_rollback_only(cls):
        return cls("Transaction commit failed because the transaction has been marked for rollback only.")

    @classmethod
    def savepoints_not_supported(cls):
        return cls("Savepoints are not supported by this driver.")

    @classmethod
    def may_not_alter_nested_transaction_with_savepoints_in_transaction(cls):
        return cls("May not alter the nested transaction with savepoints behavior while a transaction is open.")


class DBALDriverError(DBALError):
    @classmethod
    def driver_exception(cls, driver, exception):
        return cls("An exception occurred in driver '%s': %s." % (driver.get_name(), exception))

    @classmethod
    def execute_exception(cls, driver, exception, sql, params=None):
        message = "An exception occurred in driver '%s' while executing '%s'" % (driver.get_name(), sql)
        if params:
            message += " with parameters " + str(list(params))
        return cls(message + ": %s." % exception)


class DBALPlatformError(DBALError):
    @classmethod
    def not_supported(cls, method):
        return cls("Operation '%s' is not supported by platform." % method.__name__)

    @classmethod
    def invalid_isolation_level(cls, level):
        return cls("Invalid isolation level '%s'." % level)

    @classmethod
    def invalid_offset(cls, offset):
        return cls("LIMIT argument offset '%s' is not valid." % offset)

    @classmethod
    def offset_not_supported(cls, platform):
        return cls("Platform '%s' does not support offset values in limit queries." % platform.get_name())

    @classmethod
    def unknown_column_type(cls, type_):
        return cls("Unknown database type '%s' requested." % type_)


class DBALStatementError(DBALError):
    @classmethod
    def missing_positional_parameter(cls, param_index, params):
        return cls(
            "Value for positional parameter with index '%d' not found in params array: %s." % (param_index, params))

    @classmethod
    def missing_named_parameter(cls, param_name, params):
        return cls("Value for named parameter ':%s' not found in params array: %s." % (param_name, params))


class DBALBuilderError(DBALError):
    @classmethod
    def unknown_alias(cls, alias, registered_aliases):
        return cls(
            "The given alias '%s' is not part of any FROM or JOIN clause table. "
            "The currently registered aliases are: %s." % (alias, ", ".join(registered_aliases)))

    @classmethod
    def non_unique_alias(cls, alias, registered_aliases):
        return cls(
            "The given alias '%s' is not unique in FROM and JOIN clause table. "
            "The currently registered aliases are: %s." % (alias, ", ".join(registered_aliases)))


class DBALTypesError(DBALError):
    @classmethod
    def unknown_type(cls, name):
        return cls("Unknown column type '%s' requested." % name)


class DBALWarning(Warning):
    pass


class DBALNotImplementedWarning(DBALWarning):
    pass
