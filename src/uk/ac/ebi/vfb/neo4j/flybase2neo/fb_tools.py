#!/usr/bin/env python3
import psycopg2
from ..KB_tools import KB_pattern_writer
import pandas as pd

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


class FB2Neo(object):
    """A general class for moving content between FB and Neo.
    Includes connections to FB and neo4J and a generic method for running queries
    SubClass this for specific transfer jobs."""
    
    def __init__(self, endpoint, usr, pwd, file_path=''):
        """Specify Neo4J server endpoint, username and password"""
        self._init(endpoint, usr, pwd)
        self.file_path = file_path  # A path for temp csv files  # This really should be pushed up to neo4J connect (via KB tools)


    def _init(self, endpoint, usr, pwd):
        self.conn = get_fb_conn()
        pattern_writer = KB_pattern_writer(endpoint, usr, pwd)
        self.ew = pattern_writer.ew
        self.ni = pattern_writer.ni
        self.nc = pattern_writer.ni.nc
        self.fb_base_URI = 'http://www.flybase.org/reports/' # Should use curie_tools


    def query_fb(self, query):
        """Runs a query of public Flybase, 
        returns results as interable of dicts keyed on columns names"""
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
        self.conn.close()  # Investigate implementing using with statement.  Then method not required.




















    

