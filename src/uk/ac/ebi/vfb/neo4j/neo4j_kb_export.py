import sys
import os
import time  # Import time for adding delay
from pathlib import Path
from uk.ac.ebi.vfb.neo4j.KB_tools import kb_owl_edge_writer
from uk.ac.ebi.vfb.neo4j.neo4j_tools import results_2_dict_list

# Function to establish a new connection
def get_new_connection(kb, user, password):
    edge_writer = kb_owl_edge_writer(kb, user, password)
    return edge_writer.nc

# Function to execute a query
def query(query_str, kb, user, password):
    print('Q: ' + query_str)
    nc = get_new_connection(kb, user, password)  # Create new connection for each query
    q = nc.commit_list([query_str])
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
    q_count = 'MATCH (n:Entity) RETURN count(*) AS count'
    result = query(q_count, kb, user, password)
    return result[0]['count']

# Function to clear query caches
def clear_query_caches(kb, user, password):
    print("Clearing query caches")
    query("CALL db.clearQueryCaches();", kb, user, password)
    # Run a system command to suggest GC
    query("CALL dbms.listTransactions();", kb, user, password)  # Force transaction cleanup
    time.sleep(5)  # Give Neo4j time to process GC

# Function to export entities in chunks
def export_entities(kb, user, password, entity_count, outfile, delay=30):
    page_size = min(5000, (entity_count // 10) + 10) # Smaller chunks, more iterations
    page_start = 0
    page_count = 0

    file_path = Path(outfile)
    file_name_without_extension = file_path.stem
    file_extension = file_path.suffix
    parent_directory = file_path.parent

    while page_start < entity_count:
        print(f"Processing page {page_count}")
        q_generate = f'CALL ebi.spot.neo4j2owl.exportOWLNodes({page_start},{page_size})'
        o = query(q_generate, kb, user, password)[0]['o']
        part_name = f"{file_name_without_extension}_part_{page_count}{file_extension}"
        part_path = os.path.join(parent_directory, part_name)
        write_ontology(o, part_path)

        # Clear query caches between chunks
        clear_query_caches(kb, user, password)

        page_count += 1
        page_start += page_size

        # Add delay to allow Neo4j to clean up memory
        time.sleep(delay)

# Function to export relations in chunks
def export_relations(kb, user, password, outfile, delay=2):
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
        print(f"Exporting {relation_type}")
        q_generate = f'CALL ebi.spot.neo4j2owl.exportOWLEdges("{relation_type}", 0, 1)'
        o = query(q_generate, kb, user, password)[0]['o']
        out_name = f"{file_name_without_extension}_{file_suffix}{file_extension}"
        write_ontology(o, os.path.join(parent_directory, out_name))

        # Clear query caches between relation type exports
        clear_query_caches(kb, user, password)

        # Add delay to allow Neo4j to clean up memory
        time.sleep(delay)

    # Generate annotation properties and object properties in chunks
    chunk_count = 20
    for relation_type in ["annotationProperty", "objectProperty"]:
        for i in range(chunk_count):
            print(f"Exporting {relation_type} chunk {i}")
            q_generate = f'CALL ebi.spot.neo4j2owl.exportOWLEdges("{relation_type}", {i}, {chunk_count})'
            o = query(q_generate, kb, user, password)[0]['o']
            out_name = f"{file_name_without_extension}_rels_{relation_type}_{i}{file_extension}"
            write_ontology(o, os.path.join(parent_directory, out_name))

            # Clear query caches between property type chunks
            clear_query_caches(kb, user, password)

            # Add delay to allow Neo4j to clean up memory
            time.sleep(delay)

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
