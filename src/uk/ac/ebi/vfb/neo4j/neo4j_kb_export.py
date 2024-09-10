import sys
import os
from pathlib import Path
from uk.ac.ebi.vfb.neo4j.KB_tools import kb_owl_edge_writer
from uk.ac.ebi.vfb.neo4j.neo4j_tools import results_2_dict_list

# Function to establish a new connection
def get_new_connection(kb, user, password):
    edge_writer = kb_owl_edge_writer(kb, user, password)
    return edge_writer.nc

# Function to execute a query
def query(query, nc):
    print('Q: ' + query)
    q = nc.commit_list([query])
    if not q:
        return False
    dc = results_2_dict_list(q)
    if not dc:
        return False
    else:
        return dc

# Function to write ontology data to a file
def write_ontology(ont, path):
    with open(path, 'w') as text_file:
        if isinstance(ont, list):
            for chunk in ont:
                text_file.write(chunk)
        else:
            text_file.write(ont)

# Function to get entity count
def get_entity_count(kb, user, password):
    nc = get_new_connection(kb, user, password)
    q_count = 'MATCH (n:Entity) RETURN count(*) AS count'
    result = query(q_count, nc)
    nc.close()  # Drop the connection
    return result[0]['count']

# Function to export entities in chunks
def export_entities(kb, user, password, entity_count, outfile):
    page_size = (entity_count // 4) + 10  # Create 4 chunks
    page_start = 0
    page_count = 0

    file_path = Path(outfile)
    file_name_without_extension = file_path.stem
    file_extension = file_path.suffix
    parent_directory = file_path.parent

    while page_start < entity_count:
        nc = get_new_connection(kb, user, password)  # Re-establish connection for each chunk
        q_generate = f'CALL ebi.spot.neo4j2owl.exportOWLNodes({page_start},{page_size})'
        o = query(q_generate, nc)[0]['o']
        part_name = f"{file_name_without_extension}_part_{page_count}{file_extension}"
        part_path = os.path.join(parent_directory, part_name)
        write_ontology(o, part_path)
        nc.close()  # Drop the connection after each chunk

        page_count += 1
        page_start += page_size

# Function to export relations in chunks
def export_relations(kb, user, password, outfile):
    file_path = Path(outfile)
    file_name_without_extension = file_path.stem
    file_extension = file_path.suffix
    parent_directory = file_path.parent

    # List of relation types to export
    relation_types = [
        ("subclassOf", "rels_0"),
        ("instanceOf", "rels_1")
    ]
    
    # Export subclassOf and instanceOf edges
    for relation_type, file_suffix in relation_types:
        nc = get_new_connection(kb, user, password)  # Re-establish connection
        q_generate = f'CALL ebi.spot.neo4j2owl.exportOWLEdges("{relation_type}", 0, 1)'
        o = query(q_generate, nc)[0]['o']
        out_name = f"{file_name_without_extension}_{file_suffix}{file_extension}"
        write_ontology(o, os.path.join(parent_directory, out_name))
        nc.close()  # Drop connection after each chunk

    # Generate annotation properties in chunks
    chunk_count = 10
    for i in range(chunk_count):
        nc = get_new_connection(kb, user, password)  # Re-establish connection for each chunk
        q_generate = f'CALL ebi.spot.neo4j2owl.exportOWLEdges("annotationProperty", {i}, {chunk_count})'
        o = query(q_generate, nc)[0]['o']
        out_name = f"{file_name_without_extension}_rels_2_{i}{file_extension}"
        write_ontology(o, os.path.join(parent_directory, out_name))
        nc.close()  # Drop connection after each chunk

    # Generate object properties in chunks
    for i in range(chunk_count):
        nc = get_new_connection(kb, user, password)  # Re-establish connection for each chunk
        q_generate = f'CALL ebi.spot.neo4j2owl.exportOWLEdges("objectProperty", {i}, {chunk_count})'
        o = query(q_generate, nc)[0]['o']
        out_name = f"{file_name_without_extension}_rels_3_{i}{file_extension}"
        write_ontology(o, os.path.join(parent_directory, out_name))
        nc.close()  # Drop connection after each chunk

# Main execution
kb = sys.argv[1]
user = sys.argv[2]
password = sys.argv[3]
outfile = sys.argv[4]

print('Exporting KB')
entity_count = get_entity_count(kb, user, password)
print("Entity count: " + str(entity_count))
export_entities(kb, user, password, entity_count, outfile)
export_relations(kb, user, password, outfile)
