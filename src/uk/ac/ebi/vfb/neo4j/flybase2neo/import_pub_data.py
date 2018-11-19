import sys
from .pub_tools import pubMover
from .fb_tools import dict_cursor, get_fb_conn
from ..neo4j_tools import neo4j_connect, results_2_dict_list
import re

"""Populate pub data.  Should be run as a final step, once all content added."""

## TODO: Add pub types (P2)
## TODO: Add authors (P3)
## TODO: Add pub relationships (P3)

base_uri = sys.argv[1]
usr = sys.argv[2]
pwd = sys.argv[3]

nc = neo4j_connect(base_uri, usr, pwd)
pm = pubMover(base_uri, usr, pwd)

# Pull all pub FBrfs from graph
statements = ['MATCH (pub) RETURN DISTINCT pub.short_form']
results = nc.commit_list(statements)
if results:
    dc = results_2_dict_list(results)
    pub_list = [d['pub.short_form'] for d in dc]
else:
    pub_list = []

pm.move(pub_list)


# Ways to extend:  
##  Add authors
##  Add pub relationships, pub types... (via FBcv typing)
