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

kb=sys.argv[1]
user=sys.argv[2]
password=sys.argv[3]
outfile=sys.argv[4]

#kb='http://localhost:7474'
#user='neo4j'
#password='neo4j/neo'
#outfile='/data/vfb-kb.owl'

edge_writer = kb_owl_edge_writer(kb, user, password)
nc = edge_writer.nc

print('Exporting KB')
q_generate = 'CALL ebi.spot.neo4j2owl.exportOWL()'
o = query(q_generate,nc)[0]['o']

with open(outfile, 'w') as text_file:
    for chunk in o:
        text_file.write(chunk)
