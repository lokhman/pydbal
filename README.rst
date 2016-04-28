pyDBAL
======

Database Abstraction Layer (**DBAL**) for Python 2.6+.

pyDBAL library is the improved and optimised port of `Doctrine
DBAL <http://www.doctrine-project.org/projects/dbal.html>`__ project.

Installation
------------

.. code-block:: bash

    $ pip install pydbal

Requirements
------------

For using ``mysql`` driver ``MySQLdb`` library is required. Optionally
``lrucache`` is required to maintain memory safe cache operations.

Basic Usage
-----------

To open new connection import ``Connection`` from ``pydbal.connection``
package and initialise ``Connection`` class for a required driver with
desired parameters.

.. code-block:: python

    from pydbal.connection import Connection

    conn = Connection('mysql', host='localhost', user='root', database='mydb')

pyDBAL currently supports the following drivers: ``mysql`` and
``sqlite``. You can create a custom driver by inheriting
``pydbal.drivers.BaseDriver`` and passing to ``Connection`` constructor.

Query Statements
~~~~~~~~~~~~~~~~

To **SELECT** data from the database you may use ``query`` method. This
method will return the instance of ``pydbal.statement.Statement``.

.. code-block:: python

    # simple fetch generator
    for row in conn.query('SELECT * FROM table'):
        print(row)

    # same as the above but fetch mode can be applied (Connection.FETCH_*)
    for row in conn.query('SELECT * FROM table').iterate(fetch_mode=Connection.FETCH_OBJECT):
        print(row)

    # fetch row by row
    result = conn.query('SELECT * FROM table')
    row1 = result.fetch()
    row2 = result.fetch()

    # fetch all rows
    rows = conn.query('SELECT * FROM table').fetch_all()

    # fetch single value from column
    count = conn.query('SELECT COUNT(*) FROM table').fetch_column()

    # fetch all values from column by index
    ids = conn.query('SELECT id FROM table').fetch_all(fetch_mode=Connection.FETCH_COLUMN, column_index=0)

Execute Statements
~~~~~~~~~~~~~~~~~~

To execute **INSERT**, **UPDATE** or **DELETE** statements you may use
``execute`` method. This method will return number of affected rows.

.. code-block:: python

    # INSERT
    conn.execute('INSERT INTO table VALUES (?)', [val1, val2, val3])
    last_insert_id = conn.last_insert_id()

    # UPDATE
    affected_rows = conn.execute('UPDATE table SET column = ? WHERE id = ?', val1, id_)

    # DELETE
    affected_rows = conn.execute('DELETE FROM table WHERE id = ?', id_)

Statement Parameters
~~~~~~~~~~~~~~~~~~~~

Both ``query`` and ``execute`` methods support safe parameter binding by
passing arguments after the first ``sql`` argument.

.. code-block:: python

    # single positional parameter
    row = conn.query('SELECT * FROM table WHERE id = ?', id_).fetch()

    # multiple positional parameters
    row = conn.query('SELECT * FROM table WHERE id = ? OR id = ?', id1, id2).fetch()

    # named parameters
    row = conn.query('SELECT * FROM table WHERE id = :id1 OR id = :id2', id1=id1, id2=id2).fetch()

    # iterable parameters
    row = conn.query('SELECT * FROM table WHERE id IN (?)', [id1, id2]).fetch()

Transactions
~~~~~~~~~~~~

pyDBAL supports transactional operations.

.. code-block:: python

    conn.begin_transaction()
    try:
        # ... execute statements ...
        conn.commit()
    except:
        conn.rollback()

    # same as the above
    def trans():
        # ... execute statements ...
        return smth
    smth = conn.transaction(trans)

If database platform supports *savepoints* you may enable and use nested
transactions.

.. code-block:: python

    conn.set_nest_transactions_with_savepoints(True)
    conn.begin_transaction()
    # ... execute statements 1 ...
    conn.begin_transaction()
    # ... execute statements 2 ...
    conn.commit()  # commit 1
    conn.rollback()  # rollback 2

    # to control savepoints manually
    conn.create_savepoint('MYSAVEPOINT')
    conn.release_savepoint('MYSAVEPOINT')
    conn.rollback_savepoint('MYSAVEPOINT')

SQL Builder
~~~~~~~~~~~

To make writing SQL statements more simple and flexible it's suggested
to use ``pydbal.builder.SQLBuilder``.

.. code-block:: python

    # SELECT
    sqb = (
        conn.sql_builder()
            .select('t1.id', 't2.id', 'SUM(t1.col) AS special')
            .from_('table1', 't1')
            .join('t1', 'table2', 't2', 't2.id = t1.id')
            .where('t1.col = :val')
            .set_parameter('val', val)
            .group_by('t1.col')
            .having('special IS NOT NULL')
            .order_by('t2.id')
    )
    for row in sqb.execute():
        print(row)

    # INSERT
    last_insert_id = (
        conn.sql_builder()
            .insert('table')
            .values({'col1': val1, 'col2': val2})
    ).execute()

    # UPDATE
    affected_rows = (
        conn.sql_builder()
            .update('table')
            .set('col1', val1)
            .set('col2', val2)
            .where('id = :id')
            .set_parameter('id', id_)
    ).execute()

    # DELETE
    affected_rows = (
        conn.sql_builder()
            .delete('table')
            .where('id = ?')
            .set_parameter(0, id_)
    ).execute()

Expression Builder
~~~~~~~~~~~~~~~~~~

``WHERE``, ``HAVING`` and ``JOIN ... ON`` expressions can be created
using ``pydbal.builder.ExpressionBuilder``.

.. code-block:: python

    expr = conn.get_expression_builder()
    # or via SQL Builder instance
    # expr = sqb.expr()

    sqb.where(
        expr.and_x(expr.eq('a', 'b'), expr.is_null('c'))
            .or_x(
                expr.and_x('d IS NULL', expr.in_('e', ['1', '2', '3'])),
                expr.neq('f', expr.literal('abc'))
            )
    )

Schema Manager
~~~~~~~~~~~~~~

pyDBAL comes with simple read only SQL schema manager. It supports
listing of databases, tables, views, columns, indexes and foreign keys.
Internal database queries are cached with ``pydbal.cache`` mechanisms.

.. code-block:: python

    sm = conn.get_schema_manager()

    # database names
    db_names = sm.get_database_names()

    # views
    views = sm.get_views()
    view_names = sm.get_view_names()

    # tables
    tables = sm.get_tables()
    table_names = sm.get_table_names()

    # columns
    table_columns = sm.get_table_columns('table')
    table_column_names = sm.get_table_column_names('table')

    # indexes
    table_indexes = sm.get_table_indexes('table')
    table_index_names = sm.get_table_index_names('table')

    # foreign keys
    table_foreign_keys = sm.get_table_foreign_keys('table')
    table_foreign_key_names = sm.get_table_foreign_key_names('table')

License
-------

Library is available under the MIT license. The included LICENSE file
describes this in detail.
