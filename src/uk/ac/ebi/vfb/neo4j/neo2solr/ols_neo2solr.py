from ..neo4j_tools import neo4j_connect, results_2_dict_list
import json
import requests
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("pdb_endpoint",
                    help="Endpoint for connection to neo4J prod")


parser.add_argument("solr_endpoint",
                    help="Endpoint for connection to SOLR")

args = parser.parse_args()


matches = {'datasets': "MATCH (n:DataSet:Individual) "
                       "WHERE n.production = true",
           'expression_patterns': "MATCH (n:Expression_pattern)",
           'pubs': "MATCH (n:pub)"}

with_file = open("uk/ac/ebi/vfb/neo4j/neo2solr/ols_solr_rec_query.cypher", 'r')
with_clause = with_file.read()

nc = neo4j_connect(args.pdb_endpoint, 'neo4j', 'neo4j')

for m in matches.values():
    query = ' \n'.join([m, with_clause])
    print(query)
    q = nc.commit_list([query])

    r = results_2_dict_list(q)[0].encode('utf-8')
    print(r)

#    requests.put(args.solr + json.dumps(r['flat']))
