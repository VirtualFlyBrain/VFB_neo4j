'''
Created on Mar 11, 2017

@author: davidos
'''
import os
import warnings

import psycopg2


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


def fb_chado_hosts():
    """Chado servers to try, in order: FB_CHADO_HOST (comma-separated) or FlyBase's public server.
    e.g. FB_CHADO_HOST=chado.pipeline.svc.cluster.local,chado.flybase.org uses VFB's own copy and falls back to
    FlyBase's when it is unreachable or has no flybase database (yet)."""
    hosts = [h.strip() for h in os.environ.get('FB_CHADO_HOST', '').split(',') if h.strip()]
    return hosts or ['chado.flybase.org']


def get_fb_conn():
    """Connect to the first Chado server in fb_chado_hosts() that accepts the connection."""
    hosts = fb_chado_hosts()
    for i, host in enumerate(hosts):
        try:
            return psycopg2.connect(dbname='flybase', host=host, user='flybase', connect_timeout=15)
        except psycopg2.OperationalError as e:
            if i == len(hosts) - 1:
                raise
            warnings.warn("Chado at %s unavailable (%s); trying %s" % (host, str(e).strip(), hosts[i + 1]))

