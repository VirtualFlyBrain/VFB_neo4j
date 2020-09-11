import sys
from uk.ac.ebi.vfb.neo4j.flybase2neo.pub_tools import pubMover
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, results_2_dict_list, chunks
import warnings
import argparse

"""Populate pub data.  Should be run as a final step, once all content added."""

## TODO: Add pub types (P2)
## TODO: Add authors (P3)
## TODO: Add pub relationships (P3)


parser = argparse.ArgumentParser()
parser.add_argument('--test', help='Run in test mode. '
                    'runs with limits on cypher queries and additions.',
                    action="store_true", default=False)
parser.add_argument("endpoint",
                    help="Endpoint for connection to neo4J prod")
parser.add_argument("usr",
                    help="username")
parser.add_argument("pwd",
                    help="password")
args = parser.parse_args()

nc = neo4j_connect(args.endpoint, args.usr, args.pwd)
pm = pubMover(args.endpoint, args.usr, args.pwd)

limit = ''
if args.test:
    limit = 'limit 25'

# Pull all pub FBrfs from graph
statements = ['MATCH (pub:pub) WHERE pub.short_form =~ "FBrf[0-9]{7}" RETURN DISTINCT pub.short_form ' + limit]
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
        warnings.warn("No pubs found in %s" % args.endpoint)
else:
    warnings.warn("No pubs found in %s" % args.endpoint)




# Ways to extend:  
##  Add authors
##  Add pub relationships, pub types... (via FBcv typing)
