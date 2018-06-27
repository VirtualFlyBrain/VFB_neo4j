'''
Created on May 15, 2018

@author: matentzn
'''
import sys
from uk.ac.ebi.vfb.neo4j.KB_tools import kb_owl_edge_writer
from uk.ac.ebi.vfb.neo4j.neo4j_tools import results_2_dict_list
#from ..curie_tools import map_iri

def query(query,nc):
    print('Q: '+query)
    q = nc.commit_list([query])
    if not q:
        return False
    dc = results_2_dict_list(q)
    if not dc:
        return False
    else:
        return dc

def node_must_have_iri(nc):
    q_nodes_wo_iri = 'MATCH (n) WHERE NOT EXISTS(n.iri) RETURN n LIMIT 3'
    r = query(q_nodes_wo_iri,nc)
    if r!=False:
        print("FAIL: There are nodes without IRIs!")
        print(r)
    else:
        print('PASSED: node_must_have_iri[x]')

def node_must_have_nonemptyiri(nc):
    q_nodes_wo_iri = 'MATCH (n) WHERE (n.iri=\'\') RETURN n LIMIT 3'
    r = query(q_nodes_wo_iri,nc)
    if r!=False:
        print("FAIL: There are nodes without IRIs!")
        print(r)
    else:
        print('PASSED: node_must_have_nonemptyiri[x]')

def node_at_least_one_base_type(nc):
    q_nodes_wo_valid_types = 'MATCH (n) WHERE NOT n:Class AND NOT n:DataProperty AND NOT n:Individual AND NOT n:AnnotationProperty AND NOT n:ObjectProperty RETURN n LIMIT 3'
    r = query(q_nodes_wo_valid_types,nc)
    if r!=False:
        print("FAIL: There are nodes without a legal node type!")
        print(r)
    else:
        print('PASSED: node_at_least_one_base_type[x]')

def node_only_one_base_type(nc):
    q = 'MATCH (n:%s:%s) RETURN count(n) as ct'
    passed = True
    if query(q %('Class','Individual'),nc)[0]['ct']>0:
        passed = False
        print('FAIL: There are nodes that are both classes and individuals')

    if query(q %('Class','AnnotationProperty'),nc)[0]['ct']>0:
        passed = False
        print('FAIL: There are nodes that are both classes and annotation properties')

    if query(q %('DataProperty','AnnotationProperty'),nc)[0]['ct']>0:
        passed = False
        print('FAIL: There are nodes that are both data and annotation properties')

    if query(q %('DataProperty','ObjectProperty'),nc)[0]['ct']>0:
        passed = False
        print('FAIL: There are nodes that are both data and object properties')

    if query(q %('DataProperty','Class'),nc)[0]['ct']>0:
        passed = False
        print('FAIL: There are nodes that are both classes and data properties')

    if query(q %('DataProperty','Individual'),nc)[0]['ct']>0:
        passed = False
        print('FAIL: There are nodes that are both individuals and data properties')

    if query(q %('Class','ObjectProperty'),nc)[0]['ct']>0:
        passed = False
        print('FAIL: There are nodes that are both classes and object properties')

    if query(q %('ObjectProperty','Individual'),nc)[0]['ct']>0:
        passed = False
        print('FAIL: There are nodes that are both object properties and individuals')

    if query(q %('ObjectProperty','AnnotationProperty'),nc)[0]['ct']>0:
        passed = False
        print('FAIL: There are nodes that are both object properties and annotation properties')

    if query(q %('Individual','AnnotationProperty'),nc)[0]['ct']>0:
        passed = False
        print('FAIL: There are nodes that are both individuals and annotation properties')

    if passed:
        print('PASSED: node_only_one_base_type[x]')

def properties_must_have_qsl(nc):
    q_propertynodes_wo_valid_qsl = 'MATCH (n:%s) WHERE NOT EXISTS(n.qsl) RETURN n LIMIT 3'
    r = query(q_propertynodes_wo_valid_qsl % 'AnnotationProperty',nc)
    if r!=False:
        print("FAIL: There are annotation property nodes without qsl!")
        print(r)
    else:
        print('PASSED: annotation: properties_must_have_qsl[x]')

    r = query(q_propertynodes_wo_valid_qsl % 'ObjectProperty',nc)
    if r!=False:
        print("FAIL: There are object property nodes without qsl!")
        print(r)
    else:
        print('PASSED: object: properties_must_have_qsl[x]')

    r = query(q_propertynodes_wo_valid_qsl % 'DataProperty',nc)
    if r!=False:
        print("FAIL: There are data property nodes without qsl!")
        print(r)
    else:
        print('PASSED: data: properties_must_have_qsl[x]')

def rel_must_have_iri_with_valid_property(nc):
    q_invalid_qsl_relation = 'MATCH (n:Property) WITH collect(DISTINCT n.iri) as ia MATCH (o)-[r]-(q) WHERE NOT (r.iri IN ia) RETURN o,r,q LIMIT 3'
    r = query(q_invalid_qsl_relation,nc)
    if r!=False:
        print("FAIL: There are still relations with an invalid qsl type! (Note: This test requires the :Property label on all Object and AnnotationProperties)")
        print(r)
    else:
        print('PASSED: rel_must_have_iri_with_valid_property[x]')

def all_relationship_types_must_have_corresponding_property_node(nc):
    q = 'MATCH (n)-[r]-() RETURN DISTINCT type(r) as t'
    qt = 'MATCH (n:Property) WHERE n.qsl=\'%s\' RETURN n'
    r = query(q,nc)
    for t in r:
        typ = t['t']
        if typ=='Type' or typ=='SubClassOf':
            print('IGNORING: Built-in relation type %s.' % typ)
        else:
            r2 = query(qt % typ,nc)
            if r2==False:
                print("FAIL: %s does not have a corresponding property with the right qsl!" % typ)
                print(r2)
            else:
                print('PASSED: all_relationship_types_must_have_corresponding_property_node[x]')


edge_writer = kb_owl_edge_writer(sys.argv[1], sys.argv[2], sys.argv[3])
nc = edge_writer.nc

print('Collecting original database state indicators')
q_node_count = 'MATCH (n%s) RETURN count(n) as ct'
ct_nodes = query(q_node_count % '',nc)[0]['ct']
ct_class = query(q_node_count % ':Class',nc)[0]['ct']
ct_property = query(q_node_count % ':AnnotationProperty',nc)[0]['ct']
ct_individual = query(q_node_count % ':Individual',nc)[0]['ct']

print('')
print('Every node must have a non-empty IRI')
node_must_have_iri(nc)
node_must_have_nonemptyiri(nc)

print('')
print('Every node must have a valid type')
node_at_least_one_base_type(nc)

print('')
print('No node should have more that one base type')
node_only_one_base_type(nc)

print('')
print('Every property nodes must have a qsl')
properties_must_have_qsl(nc)

print('')
print('Every relationship must have an IRI related to a property')
rel_must_have_iri_with_valid_property(nc)

print('')
print('Every relationship type must have a corresponding property node')
all_relationship_types_must_have_corresponding_property_node(nc)

#all properties should be PROPERTY tagged
