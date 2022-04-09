#!/usr/bin/env python3

import requests
import json
import warnings
import re
import time
from datetime import datetime, timedelta
import math
#import token
import argparse


'''
Created on 4 Feb 2016

Tools for connecting to the neo4j REST API

@author: davidos
'''

#Could also use py2neo, but this is a bit heavy duty for some uses


def cli_credentials():
    """Parses command line credentials for Neo4J rest connection;
    Optionally specifcy additional args as a list of dicts with
    args required by argparse.add_argument().  Order in list
    specified arg order"""
    parser = argparse.ArgumentParser()
    parser.add_argument("endpoint",
                    help="Endpoint for connection to neo4J prod")
    parser.add_argument("usr",
                    help="username")
    parser.add_argument("pwd",
                    help="password")
#    if additional_args:
#        for a in additional_args:
#            parser.add_argument(**a)  # how to get this to work with non kewyord args
    return parser.parse_args()

def cli_neofj_connect():
    args = cli_credentials()
    return neo4j_connect(base_uri=args.endpoint,
                         usr=args.usr,
                         pwd=args.pwd)


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]
 

        
class neo4j_connect():
    """Thin layer over REST API to hold connection details, 
    handle multi-statement POST queries, return results and report errors."""
    # Return results might be better handled in the case of multiple statements - especially when chunked.
    # Not connection with original query is kept.
    
    
    def __init__(self, base_uri, usr, pwd):
        self.base_uri=base_uri
        self.usr = usr
        self.pwd = pwd
        self.test_connection()
       
    def commit_list(self, statements, return_graphs = False):
        """Commit a list of statements to neo4J DB via REST API.
        Prints requests status and warnings if any problems with commit.
            - statements = list of cypher statements as strings
            - return_graphs, optionally specify graphs to be returned in JSON results.
        Errors prompt warnings, not exceptions, and cause return  = FALSE.
        Returns results list of results or False if any errors are encountered."""
        cstatements = []
        if return_graphs:
            for s in statements:
                cstatements.append({'statement': s, "resultDataContents" : [ "row", "graph" ]})
        else:        
            for s in statements:
                cstatements.append({'statement': s}) # rows an columns are returned by default.
        payload = {'statements': cstatements}
        response = requests.post(url = "%s/db/data/transaction/commit" 
                                 % self.base_uri, auth = (self.usr, self.pwd) ,
                                  data = json.dumps(payload))
        if self.rest_return_check(response):
            return response.json()['results']
        else:
            return False
        
        
    def commit_list_in_chunks(self, statements, verbose=False, chunk_length=1000):
        """Commit a list of statements to neo4J DB via REST API, split into chunks.
        cypher_statments = list of cypher statements as strings
        base_uri = base URL for neo4J DB
        Default chunk size = 1000 statements. This can be overridden by KWARG chunk_length.
        Returns a list of results. Output is indistinguishable from output of commit_list (i.e. 
        chunking is not reflected in results list).
        """
        chunked_statements = chunks(l = statements, n=chunk_length)
        chunk_results = []
        i = 1
        c_no = math.ceil(len(statements)/chunk_length)
        for c in chunked_statements:
            if verbose:
                start_time = time.time()
                print("Processing chunk of %d of %d starting with: %s" % (i,
                                                                          c_no, 
                                                                          c[0].encode('utf8')))
            r = self.commit_list(c)
            if verbose:
                t = time.time() - start_time
                print("Processing took %d seconds for %s statements" % (t, len(c)))
                print("Estimated time to completion: %s." % str(timedelta(seconds=(t*(c_no - i)))))
            if type(r) == list:
                chunk_results.extend(r)
            else:
                chunk_results.append(r)
            i += 1
        return chunk_results

    def commit_csv(self, url, statement, chunk_size=1000, sep=","):
        # May need some configuration to work with file://...
        cypher = "USING PERIODIC COMMIT %d " \
                 "LOAD CSV WITH HEADERS FROM '%s' AS line FIELDTERMINATOR '%s' " \
                 "%s" % (chunk_size, url, sep, statement)
        self.commit_list([cypher])

    def rest_return_check(self, response):
        """Checks status response to post. Prints warnings to STDERR if not OK.
        If OK, checks for errors in response. Prints any present as warnings to STDERR.
        Returns True STATUS OK and no errors, otherwise returns False.
        """
        if not (response.status_code == 200):
            warnings.warn("Connection error: %s (%s)" % (response.status_code, response.reason))
            return False
        else:
            j = response.json()
            if j['errors']:
                for e in j['errors']:
                    warnings.warn(str(e))
                return False
            else:
                return True
            
    def test_connection(self):
        statements = ["MATCH (n) RETURN n LIMIT 1"]
        if self.commit_list(statements):
            return True
        else:
            return False
        
    def list_all_node_props(self):
        r = self.commit_list(['MATCH (n) with keys(n) AS kl UNWIND kl as k RETURN DISTINCT k'])
        d = results_2_dict_list(r)
        return [x['k'] for x in d]
    
    def list_all_edge_props(self):
        r = self.commit_list(['MATCH ()-[r]-() with keys(r) AS kl UNWIND kl as k RETURN DISTINCT k'])
        d = results_2_dict_list(r)
        return [x['k'] for x in d]
        
def results_2_dict_list(results):
    """Takes JSON results from a neo4J query and turns them into a list of dicts.
    """
    dc = []
    for n in results:
        # Add conditional to skip any failures
        if n:
            for d in n['data']:
                dc.append(dict(zip(n['columns'], d['row'])))
    return dc

def escape_string(strng):
    if type(strng) == str:
        strng = re.sub(r'\\', r'\\\\', strng)
        strng = re.sub("'", "\\'", strng)
        strng = re.sub('"', '\\"', strng)        
    return strng

def dict_2_mapString(d):
    """Converts a Python dict into a cypher map string.
    Only supports values of type: int, float, list, bool, string."""
    # Surely one of the fancier libraries comes with this built in!
    map_pairs = []
    for k,v in d.items():  
        if type(v) == (int):
            map_pairs.append("%s : %d" % (k,v))
        elif type(v) == float:   
            map_pairs.append("%s : %f " % (k,v))                   
        elif type(v) == str:
            map_pairs.append('%s : "%s"' % (k, escape_string(v)))           
        elif type(v) == list:                        
            map_pairs.append('%s : %s' % (k, str([escape_string(i) for i in v])))
        elif type(v) == bool:
            map_pairs.append("%s : %s" % (k, str(v)))                
        else: 
            warnings.warn("Can't use a %s as an attribute value in Cypher. Key %s Value :%s" 
                          % (type(v), k, (str(v))))
    
    return "{ " + ' , '.join(map_pairs) + " }"

class neo4jContentMover:
    """A wrapper for methods that safely move content between two neo4J databases.
    Limitation:  The database being pulled from must be Neo4j 3.n + (2.n lacks
    the properties function used here)."""
    
    def __init__(self, From, To):
        """From: a neo4jConnect object for interacting with a neo4j DB to pull content from (neo 3.n+)
        To: a neo4jConnect object for interacting with a neo4j DB to load content (2.n+)"""
        self.From = From
        self.To = To
        
    def move_nodes(self, match, key, chunk_length = 2000, verbose = True, test_mode = False):
        """match = any match statement in which a node to move is specified with variable n.
        key = attribute used in merge statements to non-redundantly add content. must be present
        in matched nodes.
        Optionally set commit chunk length, verbosity, test mode (limit 100)

        WARNING: THIS DEPENDS ON MATCH BETWEEN SETS OF LABELS. => potential danger of duplicate content.
        """
        # TODO: modify this so that MERGE does not specify label.  Then adds labels string afterwards.
        
        ret = " RETURN n.%s AS key, labels(n) AS labels , " \
                "properties(n) as properties" % key
        

        if test_mode:
            ret += " limit 100"   
        results = self.From.commit_list([match + ret])                                            
        nodes = results_2_dict_list(results)
        s = []
        for n in nodes:
            attribute_map = dict_2_mapString(n['properties'])
            label_string = ':'.join(n['labels'])
            s.append('MERGE (n:%s { %s : "%s" }) SET n = %s' % (label_string, 
                                                                  key, n['properties'][key], 
                                                                  attribute_map)) 
        self.To.commit_list_in_chunks(statements = s,
                                      verbose = verbose,
                                      chunk_length = chunk_length)
                    
    def move_edges(self, match, node_key, edge_key='', chunk_length=2000,
                   verbose=True, test_mode=False, fail_mode = 'soft'):
        """
        Identifies edges in 'from' based on match statement;
        Merges identified edges in to 'to' using combination of specified key match and
        neo4j labels of matched nodes.
        match = any match statement in which an edge (triple) is specified with variables s,r,o
        node_key = key used to match/merge to add new content
        Optionally set commit chunk length.
        """

        ret = "RETURN s.%s AS subject, labels(s) as slab, type(r) AS reltype, " \
                "properties(r) AS relprops, o.%s AS object, labels(o) AS olab " % (node_key, node_key)
        if test_mode:
            ret += " limit 100"
        results = self.From.commit_list([match + ret])
        if not results:
            if fail_mode == 'hard':
                raise Exception("fubar")
            elif fail_mode == 'soft':
                warnings.warn("Commit to KB return false. Likely connection or cypher error.")
        else:
            edges = results_2_dict_list(results)
            s = []
            for e in edges:
                attribute_map = dict_2_mapString(e['relprops'])
                rel = e['reltype']
                slab_string = ':'+':'.join(e['slab'])
                olab_string = ':'+':'.join(e['olab'])
                if edge_key:
                    if edge_key in e['relprops'].keys():
                        edge_restriction = "{ %s : '%s' }" % (edge_key, e['relprops'][edge_key])
                    else:
                        # Make this into an exception?
                        warnings.warn("Matched edge lacks specified edge_key (%s)" % (edge_key))
                        continue
                else:
                    edge_restriction = ""
                ### Move edge only when subject and object nodes match on keys and labels.
                emerge = "MATCH (s%s { %s : '%s'}), " \
                         " (o%s { %s : '%s'}) " \
                         "MERGE (s)-[r:%s %s]->(o) " % \
                                        (slab_string, node_key, e['subject'],
                                         olab_string, node_key, e['object'],
                                         rel, edge_restriction
                                         )
                if e['relprops']:
                    emerge = emerge + "SET r = %s" % attribute_map
                s.append(emerge)
            self.To.commit_list_in_chunks(statements=s,
                                          verbose=verbose,
                                          chunk_length=chunk_length)

    def move_node_labels(self, match, node_key, chunk_length=2000, verbose=True):
        """match = any match statement in which a node to move is specified with variable n.

        Look up labels for all nodes found by specified match in both From and To.
        For any case where a matched node (defined by node_key + shared labels) has labels in From but not To,
        move those labels."""

        ret = " return labels(n) as labs, n.%s" % node_key
        From_results = self.From.commit_list([match + ret])
        To_results = self.To.commit_list([match + ret])

        def roll_label_lookup(results):
            dc = results_2_dict_list(results)
            out = {}
            for d in dc:
                out[d['n.%s' % node_key]] = set(d['labs'])
            return out

        From_label_lookup = roll_label_lookup(From_results)
        TO_label_lookup = roll_label_lookup(To_results)

        statements = set()

        for k, from_labels in From_label_lookup.items():
            if k in TO_label_lookup.keys():
                From_only = from_labels - TO_label_lookup[k] # find labels that are on this node in From, not to
                both = from_labels & TO_label_lookup[k] # Find labels that are on both nodes to use in Match
                if From_only:
                    match_lab_string = ''
                    from_only_lab_string = ':'+':'.join(From_only)
                    if both:
                        match_lab_string = ':'+':'.join(both)
                    statements.add("MATCH (n%s) WHERE n.%s = '%s'"
                                   " SET n%s  " % (match_lab_string, node_key, k, from_only_lab_string))


        self.To.commit_list_in_chunks(statements=list(statements),
                                      verbose=verbose,
                                      chunk_length=chunk_length)


                      
                       
    
    
    

        
            
            


        
            
                
        
    
    
