#!/usr/bin/env python3
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--test', help='Run in test mode. ' \
                                   'runs with limits on cypher queries and additions.',
                    action="store_true")
parser.add_argument("endpoint",
                    help="Endpoint for connection to neo4J prod")
parser.add_argument("usr",
                    help="username")
parser.add_argument("pwd",
                    help="password")
args = parser.parse_args()

nc = neo4j_connect(base_uri=args.endpoint,
                   usr=args.usr, pwd=args.pwd)

limit = ''
if args.test:
    limit = ' with n limit 5 '

# Some AP deletions required for uniqueness constraints.  Needed due to quirks of OLS import.
print("Deleting depreciated...")
deletions = ["MATCH (n:VFB { short_form: 'deprecated' })-[r]-(m) DETACH DELETE n;"]
nc.commit_list(deletions)
print("uri2iri...")
uri2iri_hack = ["MATCH ()-[x]-() WHERE exists(x.uri) AND NOT exists(x.iri) SET x.iri = x.uri"]
nc.commit_list(uri2iri_hack)

print("Adding Labels...")
label_types = {
    'Neuron': ['neuron'],
    'Sensory_neuron': ['sensory neuron'],
    'Sense_organ': ['sense organ'],
    'Nervous_system': ['sense organ'],  # Fudging classification for broader search
    'Motor_neuron': ['motor neuron'],
    'Peptidergic_neuron': ['peptidergic neuron'],
    'Neuron_projection_bundle': ['neuron projection bundle'],
    'Synaptic_neuropil': ['synaptic neuropil'],
    'Synaptic_neuropil_domain': ['synaptic neuropil domain'],
    'Synaptic_neuropil_subdomain': ['synaptic neuropil subdomain'],
    'Synaptic_neuropil_block': ['synaptic neuropil block'],
    'Clone': ['neuroblast lineage clone'],
    'Cluster': ['cluster'],
    'Neuroblast': ['neuroblast'],
    'GMC': ['ganglion_mother_cell'],
    'Anatomy': ['anatomical entity'],
    'Cell': ['cell'],
    'Glial_cell': ['glial cell'],
    'Expression_pattern': ['expression pattern', 'intersectional expression pattern'],
    'Split': ['intersectional expression pattern'],
    'Ganglion': ['ganglion'],
    'Cholinergic': ['cholinergic neuron'],
    'Glutamatergic': ['glutamatergic neuron'],
    'GABAergic': ['GABAergic neuron'],
    'Octopaminergic': ['octopaminergic neuron'],
    'Dopaminergic': ['dopaminergic neuron'],
    'Serotonergic': ['serotonergic neuron'],
    'Expression_pattern_fragment': ['expression pattern fragment'],
    'Neuromere': ['neuromere'],
    'Muscle': ['Muscle cell']
}


label_additions = []
limit2 = ''
if args.test:
    limit = ' with n limit 5 '
    limit2 = ' with n, n2 limit 5 '
for k, v in label_types.items():
    label_additions.append("MATCH (n)-[r:SUBCLASSOF|INSTANCEOF*]->(n2:Class) "
                           "WHERE n2.label in %s AND NOT n:%s %s SET n:%s, n2:%s" % (str(v),
                                                                         k,
                                                                         limit2,
                                                                         k,
                                                                         k))  # Relies on coincidence of Python/Cypher list syntax

label_additions.append("MATCH (n:Individual)<-[d:Related {short_form:'depicts'}]-(ch:Individual)-[r:Related]->(fbbi:Class {label:'computer graphic'}) "
                   "WHERE NOT n:Painted_domain "
                   + limit +
                   "SET n:Painted_domain;")

label_additions.append("MATCH (n:pub) WHERE NOT n:Individual SET n:Individual")

label_additions.extend(["MATCH (n:Class) WHERE NOT n:Entity " + limit + "SET n:Entity",
                        "MATCH (n:Individual) WHERE NOT n:Entity " + limit + "SET n:Entity"])  # Entity excludes Property. Not queried

label_additions.append("MATCH (n:Feature) WHERE NOT n.self_xref='FlyBase' SET n.self_xref = 'FlyBase'")

label_additions.append("MATCH (c:Class) WHERE c.iri STARTS WITH 'http://flybase.org/reports/FB' AND NOT c.self_xref = 'FlyBase' SET c.self_xref = 'FlyBase'")

# Add Cluster label to all INSTANCES OF Cell Cluster
label_additions.append("MATCH (:Class {short_form:'VFB_10000005'})<-[:INSTANCEOF]-(n:Individual) WHERE NOT n:Cluster SET n:Cluster")

# Add labels from OWLery queries

nc.commit_list(label_additions)

print("Adding Leaf Nodes...")

# Adding leaf nodes after other classifications in place. Also needs WITH otherwise hangs.
nc.commit_list(["MATCH (n:Class:Cell) WHERE NOT n:Leaf_node AND NOT (n)<-[:SUBCLASSOF]-() "
                + limit + " WITH n SET n:Leaf_node"])

# Remove Anatomy label from Expression_pattern classes - bit hacky, but needed for correct queries in current schema
print("Removinf Anatomy from ExpPatterns...")
nc.commit_list(["MATCH (n:Expression_pattern:Class:Anatomy) REMOVE n:Anatomy"])

print("Adding Indexes...")
# Indexing - leaving off Class and Individual as these are indexed by default on OLS.
index_labels = ['Entity', 'DataSet', 'pub', 'Site', 'Expression_pattern', 'License', 'Template']

index_additions = []

for il in index_labels:
    index_additions.append("CREATE INDEX ON :%s(short_form)" % (il))

nc.commit_list(index_additions)
