"""Load the PDB into Solr (the `ontology` collection side-load).

Records are read from Neo4j in batches of ``--batch`` ids and written to Solr by ``--workers`` parallel
threads. Batches are selected by an explicit list of short_forms (``n.short_form IN $ids``) rather than
``SKIP/LIMIT`` paging, so each batch costs the same however far through the run it is (SKIP made every page
re-scan everything before it), and the batches are independent of each other and safe to run in parallel.
Documents are committed by Solr itself (``commitWithin``) with one hard commit at the end of each pass instead
of a hard commit per batch.

Environment: PDBpassword (default 'neo4j') is the Neo4j password.
"""
from ..neo4j_tools import neo4j_connect, results_2_dict_list
import argparse
import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pysolr
import resource

parser = argparse.ArgumentParser()
parser.add_argument("pdb_endpoint", help="Endpoint for connection to neo4J prod")
parser.add_argument("solr_endpoint", help="Endpoint for connection to SOLR")
parser.add_argument("--workers", type=int, default=int(os.environ.get('SIDELOAD_WORKERS', '8')),
                    help="Parallel batches in flight (default 8)")
parser.add_argument("--batch", type=int, default=int(os.environ.get('SIDELOAD_BATCH', '2000')),
                    help="Records per batch (default 2000)")
parser.add_argument("--commit-within", type=int, default=60000,
                    help="Solr commitWithin in ms for batches (default 60000); a hard commit follows each pass")
parser.add_argument("--skip-facets", action="store_true",
                    help="Skip the final facets_annotation update pass (the main passes already write facets_annotation)")
parser.add_argument("--limit", type=int, default=0,
                    help="Testing only: stop after this many records per pass (0 = all)")
args = parser.parse_args()

PDB_PASSWORD = os.environ.get('PDBpassword', 'neo4j')

# (pass name, id selection)  `n` is the node. The same node sets the original SKIP/LIMIT passes matched.
INDIVIDUALS = "n:Individual AND NOT n.short_form STARTS WITH 'VFBc_' AND NOT n.short_form STARTS WITH 'VFB_internal'"
CLASSES = "n:Class"
ENTITIES = ("n:Entity AND (n.short_form STARTS WITH 'FBbt' OR "
            "(n.short_form STARTS WITH 'VFB_' AND NOT n.short_form STARTS WITH 'VFB_internal'))")
PASSES = [('individuals', INDIVIDUALS, False), ('classes', CLASSES, False)]
FACET_PASS = ('facets', ENTITIES, True)

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "ols_solr_rec_query.cypher"), 'r') as f:
    # The file starts "WITH n SKIP 0 LIMIT 2000": the batch is now chosen by id, so the paging clause goes.
    WITH_CLAUSE = f.read().replace('SKIP 0 LIMIT 2000', '')

_local = threading.local()
_failed = []
_lock = threading.Lock()


def neo():
    if not hasattr(_local, 'nc'):
        _local.nc = neo4j_connect(args.pdb_endpoint, 'neo4j', PDB_PASSWORD)
    return _local.nc


def solr():
    if not hasattr(_local, 'solr'):
        _local.solr = pysolr.Solr(args.solr_endpoint, always_commit=False, timeout=300)
    return _local.solr


def short_forms(match):
    """All short_forms in a pass, sorted, plus the number of matching nodes that have none."""
    q = neo().commit_list([f"MATCH (n) WHERE {match} AND n.short_form IS NOT NULL "
                           f"RETURN DISTINCT n.short_form AS sf ORDER BY sf"])
    if isinstance(q, bool):
        raise RuntimeError("Query failed! could not list ids for: " + match)
    ids = [r['sf'] for r in results_2_dict_list(q)]
    q = neo().commit_list([f"MATCH (n) WHERE {match} AND n.short_form IS NULL RETURN count(n) AS c"])
    nulls = results_2_dict_list(q)[0]['c'] if not isinstance(q, bool) else 0
    return ids, nulls


def load_batch(match, ids, facets_only, label):
    query = f"MATCH (n) WHERE {match} AND n.short_form IN $ids\n" + WITH_CLAUSE
    q = neo().commit_list([{'statement': query, 'parameters': {'ids': ids}}])
    if isinstance(q, bool):
        with _lock:
            _failed.append((label, ids[0], ids[-1]))
        print("Query failed!", label, ids[0], '..', ids[-1], flush=True)
        return 0
    docs = results_2_dict_list(q)[0]['flat']
    if not docs:
        return 0
    if facets_only:
        solr().add(docs, fieldUpdates={'facets_annotation': 'set'}, commitWithin=args.commit_within)
    else:
        solr().add(docs, commitWithin=args.commit_within)
    return len(docs)


def run_pass(name, match, facets_only):
    ids, nulls = short_forms(match)
    if args.limit:
        ids = ids[:args.limit]
    print(f"maxrss after id list {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MB", flush=True)
    print(f"pass {name}: {len(ids)} records by short_form, {nulls} without a short_form", flush=True)
    batches = [ids[i:i + args.batch] for i in range(0, len(ids), args.batch)]
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(load_batch, match, b, facets_only, name) for b in batches]
        for n, fut in enumerate(as_completed(futures), 1):
            done += fut.result()          # a Solr error propagates and fails the step
            if n % 25 == 0 or n == len(futures):
                print(f"pass {name}: {n}/{len(futures)} batches, {done} records, maxrss {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MB", flush=True)
    if nulls:
        # Nodes without a short_form cannot be selected by id; the old passes carried them with id=iri.
        # There are normally none; load them the old way if there are.
        s = 0
        while True:
            query = (f"MATCH (n) WHERE {match} AND n.short_form IS NULL WITH n ORDER BY n.iri SKIP {s} LIMIT {args.batch}\n"
                     + WITH_CLAUSE.replace('WITH n  \n', '', 1).replace('WITH n \n', '', 1))
            q = neo().commit_list([query])
            if isinstance(q, bool):
                print("Query failed!", name, 'null short_form', flush=True)
                _failed.append((name, 'null', 'null'))
                break
            docs = results_2_dict_list(q)[0]['flat']
            if not docs:
                break
            solr().add(docs, commitWithin=args.commit_within, **({'fieldUpdates': {'facets_annotation': 'set'}} if facets_only else {}))
            s += args.batch
    solr().commit()                        # pass boundary: the facet updates need the documents they update
    print(f"pass {name}: committed, {done} records", flush=True)


# Delete existing documents where short_form starts with 'VFB_'. This resolves dumps steps failure to remove_embargoed_data
solr().delete(q='short_form:VFB_*', commit=True)

for name, match, facets_only in PASSES:
    run_pass(name, match, facets_only)
# adding facets (updates documents loaded above, so strictly after them):
if not args.skip_facets:
    run_pass(*FACET_PASS)

if _failed:
    print(f"Query failed! {len(_failed)} batches failed: {_failed[:5]}", flush=True)
    sys.exit(1)
print("Loading complete")
