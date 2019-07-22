import sys
from uk.ac.ebi.vfb.neo4j.flybase2neo.pub_tools import pubMover
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, results_2_dict_list, chunks
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
statements = ['MATCH (pub) RETURN DISTINCT pub.short_form'] # Needs to be shifted to short_form - coord with KB.
results = nc.commit_list(statements)
if results:
    dc = results_2_dict_list(results)
    pub_list = [d['pub.short_form'] for d in dc if d['pub.short_form']]
    if pub_list:
        print("Importing details for %d pubs." % len(pub_list))
        pub_chunks = chunks(pub_list, 1000)
        for pc in pub_chunks:
            pm.move(pc)
    else:
        warnings.warn("No pubs found in %s" % base_uri)
else:
    warnings.warn("No pubs found in %s" % base_uri)




# Ways to extend:  
##  Add authors
##  Add pub relationships, pub types... (via FBcv typing)
