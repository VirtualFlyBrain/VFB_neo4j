from ..neo4j_tools import neo4j_connect, results_2_dict_list
import json
import requests
import argparse
import pysolr

parser = argparse.ArgumentParser()

parser.add_argument("pdb_endpoint",
                    help="Endpoint for connection to neo4J prod")


parser.add_argument("solr_endpoint",
                    help="Endpoint for connection to SOLR")

args = parser.parse_args()


matches = {'individuals': "MATCH (n:Individual) WHERE NOT n.short_form STARTS WITH 'VFBc_' AND NOT n.short_form starts with 'VFB_internal' ",
           'classes': "MATCH (n:Class) "}

with_file = open("uk/ac/ebi/vfb/neo4j/neo2solr/ols_solr_rec_query.cypher", 'r')
with_clause = with_file.read()

nc = neo4j_connect(args.pdb_endpoint, 'neo4j', 'neo4j')

solr = pysolr.Solr(args.solr_endpoint, always_commit=True)
# Delete existing documents where short_form starts with 'VFB_'. This resolves dumps steps failure to remove_embargoed_data
solr.delete(q='short_form:VFB_*')

l = 2000
for m in matches.values():
    s = 0
    c = l
    while not c < l:
      query = ' \n'.join([m, with_clause.replace('SKIP 0 LIMIT 2000','SKIP ' + str(s) + ' LIMIT ' + str(l) )])
      print(query)
      q = nc.commit_list([query])
      if not isinstance(q, bool):
        r = results_2_dict_list(q)[0]
        #print(json.dumps(r, indent=4))
        c = len(r['flat'])
        print(c)
        solr.add(json.loads(json.dumps(r))['flat'])
      else:
        print("Query failed!")
        c = 0
      s += l
# adding facets:
with_file = open("uk/ac/ebi/vfb/neo4j/neo2solr/ols_solr_rec_query.cypher", 'r')
with_clause = with_file.read()
l = 2000
s = 0
c = l
while not c < l:
  query = ' \n'.join(["MATCH (n:Entity) WHERE n.short_form starts with 'FBbt' OR (n.short_form starts with 'VFB_' AND NOT n.short_form starts with 'VFB_internal') ", with_clause.replace('SKIP 0 LIMIT 2000','SKIP ' + str(s) + ' LIMIT ' + str(l) )])
  print(query)
  q = nc.commit_list([query])
  if not isinstance(q, bool):
    r = results_2_dict_list(q)[0]
    #print(json.dumps(r, indent=4))
    c = len(r['flat'])
    print(c)
    solr.add(json.loads(json.dumps(r))['flat'],fieldUpdates={'facets_annotation':'set'})
  else:
    print("Query failed!")
    c = 0
  s += l

print("Loading complete")
