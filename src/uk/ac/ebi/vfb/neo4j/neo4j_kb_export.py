import sys
import os
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from uk.ac.ebi.vfb.neo4j.KB_tools import kb_owl_edge_writer
from uk.ac.ebi.vfb.neo4j.neo4j_tools import results_2_dict_list

# Tuning knobs (environment). Defaults reproduce the original behaviour exactly: one page at a time, cache
# clearing + checkpoint after every page, 30 s / 2 s sleeps — needed on the old small KB server. On a KB with
# plenty of heap (e.g. the Kubernetes pipeline) set KB_EXPORT_WORKERS>1, KB_EXPORT_CLEAR_CACHES=false and the
# delays to 0: the output files are the same pages, just produced concurrently.
WORKERS = int(os.environ.get('KB_EXPORT_WORKERS', '1'))
CLEAR_CACHES = os.environ.get('KB_EXPORT_CLEAR_CACHES', 'true').lower() not in ('false', '0', 'no')
PAGE_DELAY = float(os.environ.get('KB_EXPORT_PAGE_DELAY', '30'))
REL_DELAY = float(os.environ.get('KB_EXPORT_REL_DELAY', '2'))
PAGE_SIZE = os.environ.get('KB_EXPORT_PAGE_SIZE')        # default: min(5000, entity_count // 50 + 10)


def log(msg):
    print(msg, flush=True)


# Function to establish a new connection
def get_new_connection(kb, user, password):
    edge_writer = kb_owl_edge_writer(kb, user, password)
    return edge_writer.nc


# Function to execute a query
def query(query_str, kb, user, password):
    log('Q: ' + query_str)
    nc = get_new_connection(kb, user, password)  # Create new connection for each query
    q = nc.commit_list([query_str])
    if not q:
        return False
    dc = results_2_dict_list(q)
    if not dc:
        return False
    else:
        return dc


def generate(q_generate, kb, user, password):
    r = query(q_generate, kb, user, password)
    if not r:
        raise RuntimeError('Export query returned nothing: ' + q_generate)
    return r[0]['o']


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
    if not CLEAR_CACHES:
        return
    log("Clearing query caches")
    query("CALL dbms.listPools();", kb, user, password)
    query("CALL dbms.queryJmx('java.lang:type=Memory') YIELD attributes RETURN attributes.HeapMemoryUsage;", kb, user, password)
    query("CALL db.clearQueryCaches();", kb, user, password)
    query("CALL db.checkpoint();", kb, user, password)
    query("CALL dbms.listTransactions();", kb, user, password)  # Force transaction cleanup
    time.sleep(5)  # Give Neo4j time to process GC


def run_jobs(jobs, delay):
    """jobs: list of (label, query, path). Sequential with clean-up + delay when WORKERS == 1 (original
    behaviour); otherwise WORKERS concurrent exports, no per-job clean-up or delay."""
    if WORKERS <= 1:
        for label, q, path in jobs:
            log(label)
            write_ontology(generate(q, kb, user, password), path)
            clear_query_caches(kb, user, password)
            time.sleep(delay)
        return

    def one(job):
        label, q, path = job
        t = time.time()
        write_ontology(generate(q, kb, user, password), path)
        log(f'{label} done in {time.time() - t:.1f}s')

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for _ in ex.map(one, jobs):     # re-raises the first failure
            pass


# Function to export entities in chunks
def export_entities(kb, user, password, entity_count, outfile, delay=PAGE_DELAY):
    page_size = int(PAGE_SIZE) if PAGE_SIZE else min(5000, (entity_count // 50) + 10)
    file_path = Path(outfile)
    jobs = []
    for page_count, page_start in enumerate(range(0, entity_count, page_size)):
        part_name = f"{file_path.stem}_part_{page_count}{file_path.suffix}"
        jobs.append((f"Processing page {page_count}",
                     f'CALL ebi.spot.neo4j2owl.exportOWLNodes({page_start},{page_size})',
                     os.path.join(file_path.parent, part_name)))
    run_jobs(jobs, delay)


# Function to export relations in chunks
def export_relations(kb, user, password, outfile, delay=REL_DELAY):
    file_path = Path(outfile)
    base = os.path.join(file_path.parent, file_path.stem)
    jobs = [(f"Exporting {rt}", f'CALL ebi.spot.neo4j2owl.exportOWLEdges("{rt}", 0, 1)', f"{base}_{suffix}{file_path.suffix}")
            for rt, suffix in [("subclassOf", "rels_0"), ("instanceOf", "rels_1")]]
    chunk_count = 50
    for rt in ["annotationProperty", "objectProperty"]:
        for i in range(chunk_count):
            jobs.append((f"Exporting {rt} chunk {i}",
                         f'CALL ebi.spot.neo4j2owl.exportOWLEdges("{rt}", {i}, {chunk_count})',
                         f"{base}_rels_{rt}_{i}{file_path.suffix}"))
    run_jobs(jobs, delay)


# Main execution
kb = sys.argv[1]
user = sys.argv[2]
password = sys.argv[3]
outfile = sys.argv[4]
log(f'Exporting KB (workers={WORKERS}, clear_caches={CLEAR_CACHES}, page_delay={PAGE_DELAY}s, rel_delay={REL_DELAY}s)')
t0 = time.time()
entity_count = get_entity_count(kb, user, password)
log("Entity count: " + str(entity_count))
export_entities(kb, user, password, entity_count, outfile)
log(f'Entities exported in {time.time() - t0:.0f}s')
export_relations(kb, user, password, outfile)
log(f'KB export finished in {time.time() - t0:.0f}s')
