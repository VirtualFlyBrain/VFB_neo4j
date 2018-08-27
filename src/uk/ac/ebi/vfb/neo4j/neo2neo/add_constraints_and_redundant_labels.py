#!/usr/bin/env python3
import sys
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect

nc = neo4j_connect(base_uri = sys.argv[1], usr = sys.argv[2], pwd = sys.argv[3])

# Some AP deletions required for uniqueness constraints.  Needed due to quirks of OLS import.

deletions = ["MATCH (n:VFB { short_form: 'deprecated' })-[r]-(m) DELETE r, n;"]
nc.commit_list(deletions)

## Run tests prior to adding uniqueness constraints:

#test = ["MATCH (n:VFB) with n.short_form as prop, collect(n) as nodelist, count(*) as count where count > 1 return prop, nodelist, count"]

#test_results = nc.commit_list(test)

# Some processing
# If test results have contents then die.

## Add constraints


# Commenting for now. constraints = ['CREATE CONSTRAINT ON (c:VFB) ASSERT c.short_form IS UNIQUE', 
#               'CREATE CONSTRAINT ON (c:VFB) ASSERT c.short_form IS UNIQUE']
# nc.commit_list(constraints)
# 
# Should really give up if constraints fail.

### Cypher query to find dups.
# "MATCH (n:VFB)
# WITH n.short_form AS prop, collect(n) AS nodelist, count(*) AS COUNT
# WHERE count > 1
# RETURN prop, nodelist, count;"

## Denormalise - adding labels for major categories:

# probably better to do by ID...
# A more flexible structure would use lists in values to allow labels from unions
# Also add label type for FlyBase feature?


label_types = {
   'Neuron': 'neuron',
   'Sensory_neuron': 'sensory neuron',
   'Motor_neuron' : 'motor neuron',
   'Peptidergic_neuron' : 'peptidergic neuron', 
   'Neuron_projection_bundle' : 'neuron projection bundle',
   'Synaptic_neuropil': 'synaptic neuropil',
   'Synaptic_neuropil_domain': 'synaptic neuropil domain',
   'Synaptic_neuropil_subdomain': 'synaptic neuropil subdomain',
   'Synaptic_neuropil_block': 'synaptic neuropil block',   
   'Clone': 'neuroblast lineage clone',
   'Cluster': 'cluster',
   'Neuroblast': 'neuroblast',
   'GMC': 'ganglion_mother_cell',
   'Anatomy': 'material anatomical entity',
   'Cell': 'cell',
   'Glial_cell': 'glial cell',
   'Expression_pattern': 'expression pattern',
   'Ganglion': 'ganglion',
   'Cholinergic': 'cholinergic neuron',
   'Glutamatergic': 'glutamatergic neuron',
   'GABAergic': 'GABAergic neuron',
   'Octopaminergic': 'octopaminergic neuron',
   'Dopaminergic': 'dopaminergic neuron',
   'Serotonergic': 'serotonergic neuron',
   'Expression_pattern_fragment': 'expression pattern fragment',
   'Neuromere': 'neuromere'
   }

label_additions = []
for k,v in label_types.items():
    label_additions.append("MATCH (n)-[r:SUBCLASSOF|INSTANCEOF*]->(n2:Class) " \
                           "WHERE n2.label = '%s' SET n:%s, n2:%s" % (v, k, k))

label_additions.append("MATCH (a:Individual)<-[d:Related]-(ch:Individual)-[r:Related]->(fbbi:Class) " \
                       "WHERE fbbi.label = 'computer graphic' and d.short_form = 'depicts' " \
                       "SET a:Painted_domain;")

nc.commit_list(label_additions)

