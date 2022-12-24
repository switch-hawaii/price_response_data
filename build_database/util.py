from __future__ import print_function
from textwrap import dedent
try:
    from io import StringIO  # Python 3
except ImportError:
    from cStringIO import StringIO  # Python 2

switch_db = 'switch'
pg_host = 'localhost'

try:
    import psycopg2
except ImportError:
    print("This module requires the psycopg2 module to access the postgresql database.")
    print("Please execute 'sudo pip install psycopg2' or 'pip install psycopg2' (Windows).")
    raise

try:
    # note: the connection gets created when the module loads and never gets closed
    # (until presumably python exits)
    con = psycopg2.connect(database=switch_db, host=pg_host, sslmode='require')

    # note: we don't autocommit because it makes executemany() very slow;
    # instead we call con.commit() after each query
    con.autocommit = False

    # note: con and cur stay open until the module goes out of scope
    cur = con.cursor()

except psycopg2.OperationalError:
    print(dedent("""
        ############################################################################################
        Error while connecting to {db} database on postgresql server {server}.
        Please ensure that your user name on the local system is the same as your postgresql user
        name or there is a local PGUSER environment variable set with your postgresql user name.
        There should also be a line like "*:*:{db}:<username>:<password>" in ~/.pgpass or
        %APPDATA%\postgresql\pgpass.conf (Windows). On Unix systems, .pgpass should be chmod 0600.
        See http://www.postgresql.org/docs/9.3/static/libpq-pgpass.html for more details.
        ############################################################################################
        """.format(db=switch_db, server=pg_host)))
    raise

def copy_dataframe_to_table(df, table):
    """
    Copy data from a pandas dataframe to the database, using psycopg2 .copy_from() function.
    Table should already exist; all columns will be written to fields with the same name;
    indexes are ignored.
    """
    # note: this could save memory by using a pipe instead of StringIO, but that would require
    # another thread, which might make debugging harder; see https://stackoverflow.com/a/9166750/3830997

    csv = StringIO()
    df.to_csv(csv, index=False, header=False)
    csv.seek(0)
    try:
        cur.copy_from(csv, table, sep=',', null='\\N', size=8192, columns=list(df.columns))
        con.commit()
    except:
        con.rollback()
        raise

def copy_table_to_dataframe(table):
    # copy data from the database to a dataframe, using psycopg2 .copy_from() function.
    # This could save memory by using a pipe instead of StringIO, but that would require
    # another thread, which might make debugging harder; see https://stackoverflow.com/a/9166750/3830997
    try:
        import pandas as pd
    except ImportError:
        print("The copy_sql_to_dataframe function requires pandas package to be installed.")
        print("Please execute 'pip install pandas' or similar.")
        raise
    csv = StringIO()
    cur.copy_to(csv, table, sep=',', null='\\N', size=8192)
    csv.seek(0)
    df = pd.read_csv(csv, index=False, header=False)


def execute(query, *args, **kwargs):
    return _execute(query, False, *args, **kwargs)

def executemany(query, *args, **kwargs):
    return _execute(query, True, *args, **kwargs)

def _execute(query, many, *args, **kwargs):
    q = dedent(query)
    func = cur.executemany if many else cur.execute
    if many:
        print(q)
    else:
        print(cur.mogrify(q, *args, **kwargs).decode())
    try:
        func(q, *args, **kwargs)
        con.commit()
        return cur
    except:
        con.rollback()
        raise
