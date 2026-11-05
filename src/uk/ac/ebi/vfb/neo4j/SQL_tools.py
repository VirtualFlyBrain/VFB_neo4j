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


# VFB's own copy of FlyBase Chado, public on 443 (Postgres 17 direct SSL; needs libpq >= 17), then FlyBase's server.
DEFAULT_CHADO_HOSTS = 'chado.flybase.virtualflybrain.org:443,chado.flybase.org'


def fb_chado_hosts():
    """Chado servers to try, in order, as host or host:port: FB_CHADO_HOST (comma-separated) or DEFAULT_CHADO_HOSTS.
    In the VFB cluster, FB_CHADO_HOST=chado.pipeline.svc.cluster.local,chado.flybase.org skips the trip through 443."""
    hosts = os.environ.get('FB_CHADO_HOST', '') or DEFAULT_CHADO_HOSTS
    return [h.strip() for h in hosts.split(',') if h.strip()]


def _conn_args(entry):
    host, _, port = entry.partition(':')
    args = {'dbname': 'flybase', 'host': host, 'user': 'flybase', 'connect_timeout': 15}
    if port:
        args['port'] = int(port)
    if port == '443':   # SNI-routed TLS on 443: TLS straight away, no Postgres SSLRequest round trip
        args.update(sslmode='require', sslnegotiation='direct')
    return args


def get_fb_conn():
    """Connect to the first Chado server in fb_chado_hosts() that accepts the connection. A server that is down, has
    no flybase database, or needs a newer libpq (sslnegotiation needs 17+) is skipped with a warning."""
    hosts = fb_chado_hosts()
    for i, entry in enumerate(hosts):
        try:
            return psycopg2.connect(**_conn_args(entry))
        except psycopg2.Error as e:
            if i == len(hosts) - 1:
                raise
            warnings.warn("Chado at %s unavailable (%s); trying %s" % (entry, str(e).strip().splitlines()[0], hosts[i + 1]))