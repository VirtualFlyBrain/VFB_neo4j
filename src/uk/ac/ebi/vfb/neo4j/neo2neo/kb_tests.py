from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, results_2_dict_list
import sys
import warnings
import json
from tqdm import tqdm

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


def compare(description: str, query1: str, query2: str, verbose=False):
    r1 = query(query1)[0]
    r2 = query(query2)[0]
    if r1['ind_count'] == r2['ind_count']:
        return None
    else:
        # print("Testing assertion:" + description)
        # print(query2)
        # print("Result: inds_in_datset: %d ; Compliant with pattern: %d" % (r1['ind_count'], r2['ind_count']))
        # log[description + '. Failing Inds'] = list(set(r1['ind_list']) - set(r2['ind_list']))
        failing_individuals = list(set(r1['ind_list']) - set(r2['ind_list']))
        return {
            'description': description,
            # 'failed_individuals': failing_individuals,
            'total_indv_count': r1['ind_count'],
            'compliant_indv_count': r2['ind_count'],
            'failed_indv_count': len(failing_individuals),
            'query': query2
        }


def dump_report_to_console(test_rpt):
    print("")
    summary = test_rpt["summary"]

    print_ratio_summary("Failed Tests", summary["failed_tests_count"], summary["total_tests_run"])
    print_ratio_summary("Failed Datasets", summary["failed_dataset_count"], summary["dataset_count"])
    print_ratio_summary("Empty Datasets", summary["empty_dataset_count"], summary["dataset_count"])
    print_ratio_summary("Successful Datasets", summary["successful_datasets_count"], summary["dataset_count"])

    print("")
    failed_datasets = test_rpt["failed_datasets"]
    if failed_datasets:
        print("=== Failed Datasets:\n")
        for failed_dataset in failed_datasets:
            print("Dataset: " + failed_dataset["dataset"])

            print("\n")


def print_ratio_summary(desc, value, total):
    print(desc + ":" + "".ljust(22-len(desc)) + str(value) + "/" + str(total) + "  \t(" + str(int(100 * value / total)) + "%)")


def dump_report_to_file(test_rpt):
    with open("kb_test.report", 'w') as report:
        report.write(json.dumps(test_rpt))


datasets = nc.commit_list(
    # ["MATCH (ds:DataSet) RETURN ds.short_form"])  # removed "WHERE ds.schema = 'image'" as not in kb2
    ["MATCH (ds:DataSet) RETURN ds.short_form LIMIT 11"])
dc = results_2_dict_list(datasets)

test_report = dict()
test_report["failed_datasets"] = list()
test_report["empty_datasets"] = list()
test_report["successful_datasets"] = list()

test_count = 0
failed_test_count = 0
return_state = True
test_progress = tqdm(dc, desc='Test Progress', total=len(dc), bar_format='{l_bar}{bar:20}| {n_fmt}/{total_fmt}')
for d in test_progress:
    log = {}
    ds = d['ds.short_form']
    dataset_status = True
    # print("\n")
    # print("Testing: " + ds)
    final_clauses = " WHERE ds.short_form = '%s' RETURN COUNT (DISTINCT i) as ind_count" \
                    ", COLLECT(i.short_form) as ind_list" % ds
    base_query = "MATCH (ds:DataSet)<-[:has_source]-(i:Individual)"
    new_base_query = "MATCH (ds:DataSet)<-[:Annotation { short_form: 'source'}]-(i:Individual)"
    if query_ind_count(base_query + final_clauses) == 0:
        if query_ind_count(new_base_query + final_clauses):
            base_query = new_base_query
            print("Using new schema for tests.")
        else:
            # print("This dataset has no content")
            test_report["empty_datasets"].append(ds)
            continue
    query1 = base_query + final_clauses
    extended_base_query = base_query + "<-[:depicts]-(j:Individual)"

    tests = list()
    tests.append({'query': extended_base_query + final_clauses,
                  'description': "All anatomical individuals in dataset have matching channel individuals.",
                  'name': 'matching_channel_test'})

    tests.append({'query': extended_base_query + "-[in_register_with]->(k:Individual)" + final_clauses,
                  'description': "All anatomical individuals in dataset have matching registered channel individuals.",
                  'name': 'registered_channel_test'})

    tests.append({'query': extended_base_query + "-[:is_specified_output_of]->(:Class)" + final_clauses,
                  'description': "All anatomical individuals in dataset have matching channel individuals with imaging method.",
                  'name': 'matching_channel_with_imaging_method_test'})

    tests.append({'query': extended_base_query + "-[:INSTANCEOF]->(c:Class { label: 'channel'})" + final_clauses,
                  'description': "All anatomical individuals in dataset have matching channel, typed individuals",
                  'name': 'matching_channel_typed_individuals_test'})

    tests.append({'query': base_query + "-[:INSTANCEOF]->(c:Class)" + final_clauses,
                  'description': "All anatomical individuals in dataset are typed.",
                  'name': 'typed_datasets_test'})

    # query2 = extended_base_query + final_clauses
    # query3 = extended_base_query + "-[in_register_with]->(k:Individual)" + final_clauses
    # query4 = extended_base_query + "-[:is_specified_output_of]->(:Class)" + final_clauses
    # query5 = extended_base_query + "-[:INSTANCEOF]->(c:Class { label: 'channel'})" + final_clauses
    # query6 = base_query + "-[:INSTANCEOF]->(c:Class)" + final_clauses

    failed_tests = list()
    for test in tests:
        result = compare(description=test['description'], query1=query1, query2=test['query'])
        test_count += 1
        if result:
            failed_tests.append({test['name']: result})
            failed_test_count += 1

    if failed_tests:
        test_report["failed_datasets"].append({
            'dataset': ds,
            'failed_tests': failed_tests
        })
    else:
        test_report["successful_datasets"].append(ds)

    test_report["summary"] = {
        'dataset_count': len(dc),
        'failed_dataset_count': len(test_report["failed_datasets"]),
        'empty_dataset_count': len(test_report["empty_datasets"]),
        'successful_datasets_count': len(test_report["successful_datasets"]),
        'total_tests_run': test_count,
        'failed_tests_count': failed_test_count
    }

dump_report_to_console(test_report)
dump_report_to_file(test_report)

if (test_report["failed_datasets"] or test_report["empty_datasets"]) and not silent_mode:
    sys.exit(1)

# KB <-> prod check numbers
