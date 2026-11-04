'''
Created on Mar 11, 2017

@author: davidos
'''
import time

import psycopg2
from psycopg2 import OperationalError


def dict_cursor(cursor):
    """Takes cursor as an input, following execution of a query, returns results as a list of dicts"""
    # iterate over rows in cursor.description, pulling first element
    description = [x[0] for x in cursor.description] 
    l = []
    for row in cursor: # iterate over rows in cursor
        d = dict(zip(description, row))
#    yield dict(zip(description, row))  # This yields an iterator.  Doesn't actually run until needed.
        l.append(d)
    return l


def get_fb_conn(max_retries=5, delay=30):
    """Connect to public FlyBase Chado, retrying if the connection is refused.

    chado.flybase.org turns clients away when it is busy ("no more connections allowed (max_client_conn)"),
    which is usually over in a minute or two. Retry max_retries times, delay seconds apart, then raise."""
    for attempt in range(1, max_retries + 1):
        try:
            return psycopg2.connect(dbname='flybase', host='chado.flybase.org', user='flybase')
        except OperationalError as e:
            if attempt == max_retries:
                raise
            print("FlyBase connection failed (%s). Retrying %d/%d in %d seconds..."
                  % (str(e).strip(), attempt, max_retries - 1, delay))
            time.sleep(delay)

