'''
Created on Mar 6, 2017

@author: davidos
'''
from warnings import warn
import re
import pandas as pd

from uk.ac.ebi.vfb.neo4j.KB_tools import kb_owl_edge_writer
from uk.ac.ebi.vfb.neo4j.neo4j_tools import results_2_dict_list
#from ..curie_tools import map_iri

edge_writer = kb_owl_edge_writer('http://localhost:7474', 'neo4j', 'neo')
dsig = pd.read_csv('uk/ac/ebi/vfb/neo4j/data_sig_vfb.csv')
nc = edge_writer.nc

# TODO MAPPING:
#DataSet: http://purl.org/dc/dcmitype/Dataset
#License: http://purl.org/dc/terms/LicenseDocument
#pub: http://purl.obolibrary.org/obo/IAO_0000311
#Template:  http://purl.obolibrary.org/obo/fbbt/vfb/VFBext_0000007
#Site: ignore for now, but we should think about mapping.
#VFB - IGNORE (edited)

print(dsig.head)
# Make sure Entities have the right type

def query(query,nc):
    print(query)
    q = nc.commit_list([query])
    if not q:
        return False
    dc = results_2_dict_list(q)
    if not dc:
        return False
    else:
        return dc
    
def transform_properties_and_relations_set_types_qsl(nc):
    q_match_properties = 'MATCH (n:Property) RETURN n'
    r = query(q_match_properties,nc)
    
    for n in r:
        iri = n['n']['iri']
        print(iri)
        q_count = 'MATCH (n)-[r {iri:\''+iri+'\'}]->(m) RETURN count(r) as ct'
        ct = query(q_count,nc)[0]['ct']
        #print('Number of relations: '+str(ct))
        
    
        qsl = dsig[['entity','qsl']].query('entity == @iri')['qsl']
        cl = dsig[['entity','etype']].query('entity == @iri')['etype']
        if qsl.count()>0 and cl.count()>0:
            qsl = qsl.iloc[0] 
            cl = cl.iloc[0]
            if 'Named' in cl:
                cl = re.sub("Named", "", cl)
            q_adjust_property = 'MATCH (p:Property {iri:\''+iri+'\'}) SET p:'+cl+' SET p.qsl = \''+qsl+'\''    
            q_rewrite_edges = 'MATCH (n)-[r {iri:\''+iri+'\'}]->(m) CREATE (n)-[r2:'+qsl+']->(m) SET r2 = r WITH r DELETE r'    
            query(q_adjust_property,edge_writer.nc)
            if ct < 60000:
                query(q_rewrite_edges,edge_writer.nc)
                
        else:
            print('ERROR: '+iri+' not defined in dataset, but has relations')
        
            

def transform_annotation_properties_on_nodes_to_array(nc):
    q = 'MATCH (n:AnnotationProperty) RETURN DISTINCT n.qsl'
    q_transform = 'MATCH (p) WHERE EXISTS(p.%s) SET p.%s=[p.%s]'
    r = query(q,nc)
    for i in r:
        qsl = i['n.qsl']
        query(q_transform % (qsl,qsl,qsl),nc)
        

print('Collecting original database state indicators')
q_node_count = 'MATCH (n%s) RETURN count(n) as ct'
ct_nodes = query(q_node_count % '',nc)[0]['ct']
ct_class = query(q_node_count % ':Class',nc)[0]['ct']
ct_property = query(q_node_count % ':Property',nc)[0]['ct']
ct_individual = query(q_node_count % ':Individual',nc)[0]['ct']
ct_undefined = query('MATCH (n) WHERE NOT n:Class AND NOT n:Individual AND NOT n:Property RETURN count(n) as ct',nc)[0]['ct']


print('Transforming properties and relations: Correct edge typing, set qsl, change edges to qsls')
transform_properties_and_relations_set_types_qsl(nc)

print('Making sure that all annotation properties are represented as arrays on nodes rather than string values. Note that this query will fail hard if the property in question is already an array')
transform_annotation_properties_on_nodes_to_array(nc)
## 

print('Dealing with annotations on nodes')

print('Dealing with annotations on edges')

print('Collecting original database state indicators')
ct_nodes_after = query(q_node_count % '',nc)[0]['ct']
ct_class_after = query(q_node_count % ':Class',nc)[0]['ct']
ct_objectproperty_after = query(q_node_count % ':ObjectProperty',nc)[0]['ct']
ct_dataproperty_after = query(q_node_count % ':DataProperty',nc)[0]['ct']
ct_individual_after = query(q_node_count % ':Individual',nc)[0]['ct']
ct_annotationproperty_after = query(q_node_count % ':AnnotationProperty',nc)[0]['ct']
ct_undefined_after = query('MATCH (n) WHERE NOT n:Class AND NOT n:Individual AND NOT n:AnnotationProperty AND NOT n:ObjectProperty AND NOT n:DataProperty RETURN count(n) as ct',nc)[0]['ct']


if ct_nodes!=ct_nodes_after:
    warn('Number of nodes has changed! (Before: %d, after: %d)' % (ct_nodes, ct_nodes_after))
    
if ct_class!=ct_class_after:
    warn('Number of classes has changed! (Before: %d, after: %d)' % (ct_class, ct_class_after))

if ct_property!=(ct_objectproperty_after+ct_annotationproperty_after+ct_dataproperty_after):
    warn('Number of properties has changed! (Before: %d, after (object): %d, after (annotation): %d, after (data):%s)' % (ct_property, ct_objectproperty_after, ct_annotationproperty_after,ct_dataproperty_after))
    
if ct_individual!=ct_individual_after:
    warn('Number of individuals has changed! (Before: %d, after: %d)' % (ct_individual, ct_individual_after))

if ct_undefined!=ct_undefined_after:
    warn('Number of undefined nodes has changed! (Before: %d, after: %d)' % (ct_undefined, ct_undefined_after))

