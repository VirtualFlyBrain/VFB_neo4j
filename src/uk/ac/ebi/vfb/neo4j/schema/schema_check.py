'''
Created on Mar 23, 2018

@author: matentzn
'''

import yaml
import warnings
import json
import flatdict
from ..neo4j_tools import neo4j_connect,  results_2_dict_list

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

def runtest(objects, testconfig, out):
    for o in objects:
        print("Testing object "+o)
        testconfig['objectlabel'] = o
        description = testconfig['description']
        q1 = testconfig['baseline'] % o
        q2 = testconfig['test'] % o
        out[o][description] = compare(label=o,description=description,query1=q1,query2=q2) 
    
def compare(label, description, query1, query2, verbose = False, write_reports = False):
    r1 = query(query1)[0]
    r2 = query(query2)[0]
    if r1['ind_count'] == r2['ind_count']:
        if verbose:
            print(query2)
            print("Testing assertion:" + description)
            print("Result: True")
        return True
    else:
        if verbose:
            print("Testing assertion:" + description)
            print(query2)
            print("Result: inds_in_datset: %d ; Compliant with pattern: %d" % (r1['ind_count'],  r2['ind_count']))
        # Should probably turn this into a report
        if write_reports:
            bad_inds = list(set(r1['ind_list']) - set(r2['ind_list']))
            file = open(label + ".report", 'w')
            file.write(json.dumps(bad_inds))
            file.close()
        return False

def to_label_list(olist):
    objects = []
    for o in olist:
        objects.append(o['label'])
    return objects

def prepare_test_results(objects):
    test_stats = {}
    for d in objects:
        test_stats[d] = {}
    return test_stats

def assemble_query(conf,var):
    query = ""
    for queryfragment in conf.split("|"):
        query += var[queryfragment]+" "
    return query

def extract_test_parameters(test, var, cypher_return_clause):
    testconfig = {}
    testconfig['description'] = test['description']
    testconfig['baseline'] = assemble_query(test['baseline'],var) + cypher_return_clause
    testconfig['test'] =  assemble_query(test['test_base'],var)+test['test']+ cypher_return_clause
    return testconfig

def run_tests(tests,testset,test_stats):
    for test in tests:
        print("Running test: "+test['description'])
        runtest(objects, extract_test_parameters(test,testset['vars'],testset['query_return']), test_stats)
    return True

config = yaml.load(open('neo_schema.yml'))
nc = neo4j_connect(config['url'], config['usr'], config['pwd'])

testsconfig = yaml.load(open('kb_schema.yml'))
testsets = testsconfig['testsets']
for testset in testsets:
    tests = testset['tests']
    objects = to_label_list(query(testset['objects']))
    test_stats = prepare_test_results(objects)
    run_tests(tests,testset,test_stats)
    
print(json.dumps(test_stats, sort_keys=True, indent=4))

failures = flatdict.FlatDict(test_stats).values()
fail = False in failures
if fail:
    failed = failures.count(False)
    print(str(failed) + " out of " + str(len(failures))+ " tests failed.")
else:
    print("All tests passed!")