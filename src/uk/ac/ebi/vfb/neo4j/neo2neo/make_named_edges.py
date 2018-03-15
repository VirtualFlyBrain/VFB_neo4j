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

# def make_name_edges(typ, s='', o='', test_mode = False):
#     if test_mode:
#         test = " limit 10"
#     else:
#         test = ""
#     """ typ = edge label.  o, s = subject and object labels. These hould be pre prepended with ':'"""
#     statements = ["MATCH (n%s)-[r:%s]->(m%s) RETURN n.short_form, r.label, m.short_form %s" % (s, typ, o, test)]
#     r = nc.commit_list(statements)        
#     triples = [x['row'] for x in r[0]['data']]
#     statements = []
#     # Iterate over, making named edges for labels (sub space for _)
#     print("Processing %d triples" % len(triples))
#     for t in triples:
#         subj = t[0]
#         rel = re.sub(' ', '_', t[1]) # In case any labels have spaces
#         obj = t[2]
#         # Merge ensures this doesn't lead to duplicated edges if already present:
#         statements.append("MATCH (n {short_form:'%s'}),(m {short_form:'%s'}) " \
#                           "MERGE (n)-[r:%s { type: '%s' }]->(m)" % (subj, obj, rel, typ)) 
#     print("processing %s %s statements" % (len(statements), typ))    
#     nc.commit_list_in_chunks(statements, verbose = True, chunk_length = 10000)
    

def make_name_edges(typ, delete_old=True, test_mode = False):
    if test_mode:
        test = " limit 10"
    else:
        test = ""
    if delete_old:
        delete = " DELETE r"
    else:
        delete = ""
    statements = ["MATCH (n)-[r:%s]->(m) RETURN  collect(distinct r.label) as labels" % (typ)]    
    r = nc.commit_list(statements)  
    statements = []
    # Iterate over, making named edges for labels (sub space for _)
    labels = r[0]['data'][0]['row'][0]
    for label in labels:
        rel = re.sub(' ', '_', label) # In case any labels have spaces
        statements.append("MATCH (n)-[r:%s {label:'%s'}]->(m) MERGE (n)-[r2:%s]->(m) SET r2=r,r2.type=type(r) %s%s" % (typ, label, rel, delete, test))    
    print("processing %s %s statements" % (len(statements), typ))    
    nc.commit_list_in_chunks(statements, verbose = True, chunk_length = 10000)
    
make_name_edges(typ='Related', test_mode = args.test)

