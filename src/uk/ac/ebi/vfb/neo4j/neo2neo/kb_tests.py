from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect,  results_2_dict_list
import sys
import warnings
import json

# KB tests

# Query for dataSets (would be helpful if these were tagged as image)

nc = neo4j_connect(sys.argv[1], sys.argv[2], sys.argv[3])

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

def compare(dataset, description, query1, query2, verbose = False):
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
        print("Result: inds_in_datset: %d ; Compliant with pattern: %d" % (r1['ind_count'],  r2['ind_count']))
        # Should probably turn this into a report
        bad_inds = list(set(r1['ind_list']) - set(r2['ind_list']))
        file = open(dataset + ".report", 'w')
        file.write(json.dumps(bad_inds))
        file.close()
        return False


datasets = nc.commit_list(["MATCH (ds:DataSet) WHERE ds.schema = 'image' RETURN ds.label"])
dc = results_2_dict_list(datasets)

return_state = True

for d in dc:
    ds = d['ds.label']
    dataset_status = True
    print("\n")
    print ("Testing: " + ds)
    final_clauses = " WHERE ds.label = '%s' RETURN COUNT (DISTINCT i) as ind_count" \
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
    extended_base_query = base_query + "<-[:Related { short_form: 'depicts' }]-(j:Individual)"
    query2 = extended_base_query + final_clauses
    query3 = extended_base_query + "-[{ iri: 'http://purl.obolibrary.org/obo/RO_0002026' }]->(k:Individual)" + final_clauses
    query4 = extended_base_query + "-[:Related { label: 'is_specified_output_of'} ]->(:Class)" + final_clauses
    query5 = extended_base_query + "-[:INSTANCEOF]->(c:Class { label: 'channel'})" + final_clauses
    query6 = base_query + "-[:INSTANCEOF]->(c:Class)" + final_clauses

    test_stats = []

    test_stats.append(compare(dataset=ds,
                              description="All anatomical individuals in dataset have matching channel individuals.",
                              query1=query1,
                              query2=query2))
    test_stats.append(compare(description="All anatomical individuals in dataset have matching registered channel individuals.",
                              dataset=ds,
                              query1=query1,
                              query2=query3))
    test_stats.append(compare(description="All anatomical individuals in dataset have matching channel individuals with imaging method",
                              dataset=ds,
                              query1=query1,
                              query2=query4))
    test_stats.append(compare(description="All anatomical individuals in dataset have matching channel, typed individuals",
                              dataset=ds,
                              query1=query1,
                              query2=query5))
    test_stats.append(compare(description="All anatomical individuals in dataset are typed",
                              dataset=ds,
                              query1=query1,
                              query2=query4))
    if False in test_stats:
        return_state = False
    else:
        print("Passes!")


if not return_state:
    sys.exit(1)

# KB <-> prod check numbers