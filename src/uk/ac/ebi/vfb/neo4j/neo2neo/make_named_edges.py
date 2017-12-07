#!/usr/bin/env python3

import sys
import re
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect
import argparse
"""A simple script to make edges named (typed) for relations from all edges of of type :Related.
Arg1 = base_uri or neo4J server
Arg2 = usr
Arg2 = pwd

This script relies on a uniqueness constraint being in place for OBO ids.

Created on 4 Feb 2016

@author: davidos"""

parser = argparse.ArgumentParser()
parser.add_argument('--test', help='Run in test mode. ' \
                    'runs with limits on cypher queries and additions.',
                    action = "store_true")
parser.add_argument("endpoint",
                    help="Endpoint for connection to neo4J prod")
parser.add_argument("usr",
                    help="username")
parser.add_argument("pwd",
                    help="password")
args = parser.parse_args()



# Current version makes all edges.  Might want to limit the types of edges made to those needed for graphing purposes.

# TODO: add in check of uniqueness constraint
# Use REST calls to /db/data/schema/




nc = neo4j_connect(base_uri = args.endpoint, 
                   usr = args.usr, pwd = args.pwd)

def make_name_edges(typ, s='', o='', test_mode = False):
    if test_mode:
        test = " limit 50"
    else:
        test = ""
    """ typ = edge label.  o, s = subject and object labels. These hould be pre prepended with ':'"""
    statements = ["MATCH (n%s)-[r:%s]->(m%s) RETURN n.short_form, r.label, m.short_form %s" % (s, typ, o, test)]
    r = nc.commit_list(statements)        
    triples = [x['row'] for x in r[0]['data']]
    statements = []
    # Iterate over, making named edges for labels (sub space for _)
    for t in triples:
        subj = t[0]
        rel = re.sub(' ', '_', t[1]) # In case any labels have spaces
        obj = t[2]
        # Merge ensures this doesn't lead to duplicated edges if already present:
        statements.append("MATCH (n),(m) " \
                          "WHERE n.short_form = '%s' and m.short_form = '%s' " \
                          "MERGE (n)-[r:%s { type: '%s' }]->(m)" % (subj, obj, rel, typ)) 
    print("processing %s %s statements" % (len(statements), typ))    
    nc.commit_list_in_chunks(statements, verbose = True, chunk_length = 10000)

make_name_edges(typ='Related', test_mode = args.test)
