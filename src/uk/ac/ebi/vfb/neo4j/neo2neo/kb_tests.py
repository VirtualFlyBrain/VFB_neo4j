from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, results_2_dict_list
import sys
import warnings
import json

nc = neo4j_connect(sys.argv[1], sys.argv[2], sys.argv[3])

silent_mode = False
# prevents sys.exit(1) on failure, just silently logs the result and exits
if len(sys.argv) > 4 and sys.argv[4] == 'silent_fail':
    silent_mode = True


def query(query):
    q = nc.commit_list([query])
    if not q:
        return False
    dc = results_2_dict_list(q)
    if not dc:
        return False
    else:
        return dc


def query_ind_count(query):
    q = nc.commit_list([query])
    if not q:
        return False
    dc = results_2_dict_list(q)
    if not dc:
        return False
    if not ('ind_count' in dc[0].keys()):
        warnings.warn("Query has no ind_count")
        return False
    else:
        return dc[0]['ind_count']


def compare(dataset: str, description: str, query1: str, query2: str, log: dict, verbose=False):
    r1 = query(query1)[0]
    r2 = query(query2)[0]
    if r1['ind_count'] == r2['ind_count']:
        if verbose:
            print(query2)
            print("Testing assertion:" + description)
            print("Result: True")
        return True
    else:
        print("Testing assertion:" + description)
        print(query2)
        print("Result: inds_in_datset: %d ; Compliant with pattern: %d" % (r1['ind_count'], r2['ind_count']))
        log[description + '. Failing Inds'] = list(set(r1['ind_list']) - set(r2['ind_list']))
        return False


datasets = nc.commit_list(
    ["MATCH (ds:DataSet) RETURN ds.short_form"])  # removed "WHERE ds.schema = 'image'" as not in kb2
dc = results_2_dict_list(datasets)

return_state = True

for d in dc:
    log = {}
    ds = d['ds.short_form']
    dataset_status = True
    print("\n")
    print("Testing: " + ds)
    final_clauses = " WHERE ds.short_form = '%s' RETURN COUNT (DISTINCT i) as ind_count" \
                    ", COLLECT(i.short_form) as ind_list" % ds
    base_query = "MATCH (ds:DataSet)<-[:has_source]-(i:Individual)"
    new_base_query = "MATCH (ds:DataSet)<-[:Annotation { short_form: 'source'}]-(i:Individual)"
    if query_ind_count(base_query + final_clauses) == 0:
        if query_ind_count(new_base_query + final_clauses):
            base_query = new_base_query
            print("Using new schema for tests.")
        else:
            print("This dataset has no content")
            continue
    query1 = base_query + final_clauses
    extended_base_query = base_query + "<-[:depicts]-(j:Individual)"
    query2 = extended_base_query + final_clauses
    query3 = extended_base_query + "-[in_register_with]->(k:Individual)" + final_clauses
    query4 = extended_base_query + "-[:is_specified_output_of]->(:Class)" + final_clauses
    query5 = extended_base_query + "-[:INSTANCEOF]->(c:Class { label: 'channel'})" + final_clauses
    query6 = base_query + "-[:INSTANCEOF]->(c:Class)" + final_clauses

    test_stats = []

    test_stats.append(compare(dataset=ds,
                              description="All anatomical individuals in dataset have matching channel individuals.",
                              query1=query1,
                              query2=query2,
                              log=log))
    test_stats.append(
        compare(description="All anatomical individuals in dataset have matching registered channel individuals.",
                dataset=ds,
                query1=query1,
                query2=query3,
                log=log))
    test_stats.append(compare(
        description="All anatomical individuals in dataset have matching channel individuals with imaging method",
        dataset=ds,
        query1=query1,
        query2=query4,
        log=log))
    test_stats.append(
        compare(description="All anatomical individuals in dataset have matching channel, typed individuals",
                dataset=ds,
                query1=query1,
                query2=query5,
                log=log))
    test_stats.append(compare(description="All anatomical individuals in dataset are typed",
                              dataset=ds,
                              query1=query1,
                              query2=query6,
                              log=log))
    if False in test_stats:
        return_state = False
        with open(ds + ".report", 'w') as report:
            report.write(json.dumps(log))
    else:
        print("Passes!")

if not return_state and not silent_mode:
    sys.exit(1)

# KB <-> prod check numbers
