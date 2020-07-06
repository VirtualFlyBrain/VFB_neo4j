'''
Created on Apr 28, 2017

@author: davidos
'''


from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, neo4jContentMover
import sys
from uk.ac.ebi.vfb.neo4j.KB_tools import node_importer

"""
A simple script to take all non OWL nodes (not :Class, :Property, or :Individual)
and all non-owl edges (not :Related, :INSTANCEOF, :SUBCLASSOF)
and move them from KB to prod.
"""

# Description of the problem:
# Nodes in KB can be uniquely identified by iri
# Nodes in prod cannot be uniquely identified by iri
# But can be uniquely identified by some combination of neo4J label + iri
# Labels are largely but not completely matched between KB and prod.
# Loading edges only to correct prod nodes therefore requires specifying edge labels in merge.
# Merge strategy 1:
# Merge requires all labels on nodes retrieved from KB to match those those on nodes with same key in prod
# Scenario 1:  Node loaded from OWL, has extra labels in KB
# KB Load will fail.
# Merge strategy 2
# If loading edge to prod loads nodes through merge => duplicate nodes.
# Solution is to align node labels from KB -> Prod prior to edge loading.
# But how to do this safely?  Can't just match on iri!
# Two options:
#   - Implement a label mover that works with match statements.  Run this with equivalent queries before
# running edge loader
#   - Break up match statement on edge mover into s, r, o chunks.  Use the s and o chunks to specify edges to move.
#     Limitation of this approach is that only simple match statements or possible
#      - with just a triple and no 'where' clause.

kb = neo4j_connect(sys.argv[1], sys.argv[2], sys.argv[3])
prod = neo4j_connect(sys.argv[4], sys.argv[5], sys.argv[6])

ni = node_importer(sys.argv[4], sys.argv[5], sys.argv[6])

ni.add_default_constraint_set(['DataSet', 'Site', 'License', 'Individual'])

ncm = neo4jContentMover(kb,prod)


# Labels may not match on nodes already loaded.  The following finds all Individuals in KB that are in Prod,
# checks for differences in labels, and adds them to the equivalent node in Prod if they match.
ncm.move_node_labels(match="MATCH (n:Individual) ",
                     node_key='iri')

## move channels and directly associated edges

## Move all instance of channel:

channel_match = "MATCH (:Class { label: 'channel'})<-[:INSTANCEOF]-(n:Individual) "
ncm.move_nodes(match=channel_match, key='iri', test_mode=False)



# Then add edges in each direction independently (could be bundled with edge mover but going with simple solution here)
ncm.move_edges(match="MATCH (:Class { label: 'channel'})<-[:INSTANCEOF]-(s:Individual) "
                     "WITH s MATCH (s)-[r]->(o) ",
               node_key='iri',
               test_mode=False)
ncm.move_edges(match="MATCH (:Class { label: 'channel'})<-[:INSTANCEOF]-(o:Individual) "
                     "WITH o MATCH (s)-[r]->(o) ",
               node_key='iri',
               test_mode=False)

## move non-OWL content
## Requires: IRIs for everything to be moved (as iri)
non_owl_node_match = "MATCH (n) " \
        "WHERE ('DataSet' IN labels(n)) " \
        "OR ('License' IN labels(n)) " \
        "OR ('Person' IN labels(n)) " \
        "OR ('Site' IN labels(n)) " \
        "OR ('pub' IN labels(n)) " \
        "OR ('API' IN labels(n)) "
           
ncm.move_nodes(match=non_owl_node_match, key='iri', test_mode=False)
 
non_owl_edge_match = "MATCH (s)-[r]->(o) " \
             "WHERE not(type(r) IN ['INSTANCEOF', 'Related', 'SUBCLASSOF']) " 
ncm.move_edges(match=non_owl_edge_match, node_key='iri', test_mode=False)


