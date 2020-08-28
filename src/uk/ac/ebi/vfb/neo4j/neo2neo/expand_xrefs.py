from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, results_2_dict_list
from uk.ac.ebi.vfb.neo4j.KB_tools import kb_owl_edge_writer

import argparse
import json

"""A simple script to expand xrefs on ontlogy classes in OLS.
Any XREF in a loaded ontology file whose DB component 
corresponds to the short_form of a Site node, is 
turned into a linkout (dbxref) to the site node, with acc
on  the edge.
Arg1 = base_uri or neo4J server
Arg2 = usr
Arg2 = pwd"""



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

nc = neo4j_connect(base_uri = args.endpoint,
                   usr = args.usr, pwd = args.pwd)

# Hack to deal with descrepancy in xref property labels between sources:

nc.commit_list(["MATCH (p:Property { short_form: 'hasDbXref' }) SET p.label = 'hasDbXref' "])

sr = nc.commit_list(["MATCH (s:Site) RETURN collect (s.short_form) as Sites"])
sites = results_2_dict_list(sr)[0]['Sites']

query = "MATCH (c:Class) WHERE exists(c.obo_xref) return c.short_form, c.obo_xref "

r = nc.commit_list([query])

dc = results_2_dict_list(r)

ew = kb_owl_edge_writer(args.endpoint, args.usr, args.pwd)

# JSON example for ref:
# obo_xref:{"database":"FlyBrain_NDB","id":"10079","description":null,"url":null}



for d in dc:
    c = d['c.short_form']
    xrefs = d['c.obo_xref']
    for x in xrefs:
        xds = json.loads(x)
        db = xds['database']
        acc = xds['id']
        if db in sites:
            ew.add_annotation_axiom(s=c,
                                    r='hasDbXref',
                                    o=db,
                                    stype=':Entity',
                                    edge_annotations={
                                        "accession": acc,
                                    },
                                    match_on='short_form',
                                    safe_label_edge=True
                                    )

ew.commit(chunk_length=1000)









