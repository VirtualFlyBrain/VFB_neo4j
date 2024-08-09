'''
Created on May 15, 2018

@author: matentzn
'''
import sys
import os
from pathlib import Path
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

def write_ontology(ont, path):
    with open(path, 'w') as text_file:
        if isinstance(ont, list):
            for chunk in ont:
                text_file.write(chunk)
        else:
            text_file.write(ont)

def get_entity_count(nc):
    q_count = 'MATCH (n:Entity) RETURN count(*) AS count'
    result = query(q_count,nc)
    return result[0]['count']

def export_entities(nc, entity_count, outfile):
    # page_size = 10000
    page_size = (entity_count // 4) + 10  # create 4 chunks
    page_start = 0
    page_count = 0 

    file_path = Path(outfile)
    file_name_without_extension = file_path.stem
    file_extension = file_path.suffix
    parent_directory = file_path.parent

    while page_start < entity_count:
        q_generate = f'CALL ebi.spot.neo4j2owl.exportOWLNodes({page_start},{page_size})'
        o = query(q_generate,nc)[0]['o']
        part_name = f"{file_name_without_extension}_part_{page_count}{file_extension}"
        part_path = os.path.join(parent_directory, part_name)
        write_ontology(o, part_path)
        page_count = page_count + 1
        page_start = page_start + page_size

def export_relations(nc, outfile):
    file_path = Path(outfile)
    file_name_without_extension = file_path.stem
    file_extension = file_path.suffix
    parent_directory = file_path.parent

    out_name = f"{file_name_without_extension}_rels{file_extension}"
    out_path = os.path.join(parent_directory, out_name)

    q_generate = 'CALL ebi.spot.neo4j2owl.exportOWLEdges()'
    o = query(q_generate,nc)[0]['o']
    write_ontology(o, out_path)

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
entity_count = get_entity_count(nc)
print("Entity count: " + str(entity_count))
export_entities(nc, entity_count, outfile)
export_relations(nc, outfile)

