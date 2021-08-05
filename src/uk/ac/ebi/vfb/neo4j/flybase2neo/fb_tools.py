#!/usr/bin/env python3
import psycopg2
from ..KB_tools import KB_pattern_writer, node_importer, kb_owl_edge_writer
import pandas as pd
from operator import itemgetter

'''
Created on 4 Feb 2016

General classes

@author: davidos
'''

### Sketch of usage:
# 1. query for expressed gene products
# generate feature, relation gp -> FBti/FBtp -> Allele (typed) -> gene
# Query genotype table -> generate genotypes and





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


def get_fb_conn():
    return psycopg2.connect(dbname='flybase',
                            host='chado.flybase.org',
                            user='flybase')


def dict_list_2_dict(key, dict_list, pfunc, sort = True):
    """Processes a list of dicts to produce a single dict.
    key = key of output dict.  Must be present in dict in dict_list
    dict_list = a list of dicts.
    pfunc = a function applied to all dicts in dict_list sharing the same key
            to produce a value for the output dict.  The type of this value is
            specified by what the function returns. This function must take 2 args:
                First arg: input dict
                Second arg: output datastructure (or False)
            If the output datastructure is (inrepreted as) False
            it must generate new one otherwise it should generate a new output
            datastructure
    sort = Boolean to set sorting of the input dict_list on value of output key.
    The default = True as this sorting is required for the function to work, but it can be set to False for purposes of efficiency if the input is already pre-sorted.
    """
    if sort:
        dict_list = sorted(dict_list, key=itemgetter(key))
    result = {}
    out = False
    old_key = ''
    while dict_list:
        d = dict_list.pop()
        current_key = d[key]
        if not current_key == old_key:
            out = False
        out = pfunc(d, out)
        result[current_key] = out # We can reassign every time as out is mutating.
        old_key = current_key
    return result

import time

class FB2Neo(object):
    """A general class for moving content between FB and Neo.
    Includes connections to FB and neo4J and a generic method for running queries
    SubClass this for specific transfer jobs."""
    
    def __init__(self, endpoint, usr, pwd, file_path='', fb_conn_reset_time=300):
        """Specify Neo4J server endpoint, username and password"""
        self._init(endpoint, usr, pwd, fb_conn_reset_time=fb_conn_reset_time)
        self.file_path = file_path  # A path for temp csv files  # This really should be pushed up to neo4J connect (via KB tools)


    def _init(self, endpoint, usr, pwd, fb_conn_reset_time = 300):
        self.conn = get_fb_conn()
        self.conn_start_time = time.time()
        self.conn_reset_time = fb_conn_reset_time
        self.ew = kb_owl_edge_writer(endpoint, usr, pwd)
        self.ni = node_importer(endpoint, usr, pwd)
        self.nc = self.ni.nc
        self.fb_base_URI = 'http://www.flybase.org/reports/' # Should use curie_tools


    def query_fb(self, query):
        """Runs a query of public Flybase, 
        returns results as interable of dicts keyed on columns names"""
        if time.time() - self.conn_reset_time:
            self.conn.close()
            self.conn = get_fb_conn()
            self.conn_start_time = time.time()
        cursor = self.conn.cursor()  # Investigate using with statement
        cursor.execute(query)
        dc = dict_cursor(cursor)
        cursor.close()
        return dc

    def commit_via_csv(self, statement, dict_list):
        df = pd.DataFrame.from_records(dict_list)
        df.to_csv(self.file_path + "tmp.csv", sep='\t')
        self.nc.commit_csv("file:///" + "tmp.csv",
                           statement=statement,
                           sep="\t")
        # add something to delete csv here.

        
    def close(self):
        self.conn.close()




















    

