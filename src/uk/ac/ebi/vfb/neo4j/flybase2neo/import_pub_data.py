import sys
from .pub_tools import pubMover
from .fb_tools import dict_cursor, get_fb_conn
from ..neo4j_tools import neo4j_connect, results_2_dict_list
import re
import warnings

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
statements = ['MATCH (pub) RETURN DISTINCT pub.FlyBase'] # Needs to be shifted to short_form - coord with KB.
results = nc.commit_list(statements)
if results:
    dc = results_2_dict_list(results)
    pub_list = [d['pub.FlyBase'] for d in dc if d['pub.FlyBase']]
    if pub_list:
        pm.move(pub_list)
    else:
        warnings.warn("No pubs found in %s" % base_uri)
else:
    warnings.warn("No pubs found in %s" % base_uri)




# Ways to extend:  
##  Add authors
##  Add pub relationships, pub types... (via FBcv typing)
