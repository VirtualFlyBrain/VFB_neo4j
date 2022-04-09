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
file=sys.argv[4]

#kb='http://localhost:7474'
#user='neo4j'
#password='neo4j'
#file='/data/txyz.owl'

#edge_writer = kb_owl_edge_writer(sys.argv[1], sys.argv[2], sys.argv[3])
edge_writer = kb_owl_edge_writer(kb, user, password)
nc = edge_writer.nc

print('Exporting KB 2 OWL')
res = query('CALL ebi.spot.neo4j2owl.exportOWL()',nc)
owl = res[0]['o']
#print(owl)
print(res[0]['log'])


with open(file, "w") as text_file:
    print(owl, file=text_file)