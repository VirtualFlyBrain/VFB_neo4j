from uk.ac.ebi.vfb.neo4j.neo4j_tools import results_2_dict_list, escape_string
from uk.ac.ebi.vfb.curie_tools import map_iri
import sys
import json
import warnings
import argparse
from uk.ac.ebi.vfb.neo4j.KB_tools import kb_owl_edge_writer, node_importer
import re

parser = argparse.ArgumentParser()
parser.add_argument('--test', help='Run in test mode. '
                    'runs with limits on cypher queries and additions.',
                    action="store_true", default=False)
parser.add_argument("endpoint",
                    help="Endpoint for connection to neo4J prod")
parser.add_argument("usr",
                    help="username")
parser.add_argument("pwd",
                    help="password")
args = parser.parse_args()


"""
Converts references on definitions and synonyms, stored as entity attributes
in OLS Neo4j, into edges linked to pub nodes.  In the case of synonyms, 
edges store synonym names, scopes and types. 

Background: 

OLS Neo4J includes references attached to definitions and synonyms, 
but these are packed into JSON strings on attributes.

Almost every reference has an FBrf, but a few only have PMIDS or DOIs. 
Merge strategy uses FBrf first, then PMID, then DOI."""


# Plan
# New edge writing should use ew
# Populate pubs as you go
# Write loop
# 1. finds Defs & Syns
# unpacks; Finds FlyBase (& DOI?) refs
#

# def roll_cypher_add_def_pub_link(sfid, pub_id):
#     """Generates a Cypher statement that links an existing class
#     to a pub node with the specified attribute.  Generates a new pub node
#      if none exists."""
#     return "MATCH (a:Class { short_form : \"%s\" }) " \
#            "MERGE (p:pub:Individual { short_form : \"%s\" }) " \
#            "MERGE (a)-[:has_reference { typ : \"def\" }]->(p)" % (sfid, pub_id)
#
#
# def roll_cypher_add_syn_pub_link(sfid, s, pub_id_typ, pub_id):
#     """Generates a Cypher statement that links an existing class
#     to a pub node ..."""
#     label = re.sub("'", "\'", s['name'])
#     return "MATCH (a:Class { short_form : \"%s\" }) " \
#            "MERGE (p:pub:Individual { short_form : \"%s\" }) " \
#            "MERGE (a)-[:has_reference { typ : \"syn\", scope: \"%s\", " \
#            "synonym : \"%s\", cat: \"%s\" }]->(p)" \
#            "" % (sfid, pub_id, s['scope'], label, s['type'])


#["{ "value": "CoA", "annotations": {"database_cross_reference": [ "FlyBase:FBrf0112030" ]}}", "{ "value": "cousin of aCC", "annotations": {"database_cross_reference": [ "FlyBase:FBrf0112030" ]}}"]

class pubLink():

    def __init__(self, endpoint, usr, pwd, test_mode=False):
        self.edge_witer = kb_owl_edge_writer(endpoint, usr, pwd)
        self.node_writer = node_importer(endpoint, usr, pwd)
        self.test_mode=test_mode


 # TODO: support unnatributed for synonyms by adding a link to 'unattributed'


    def write_pub_link(self, subject, json_string, type, supported_xrefs = ('FlyBase')):

        """Exapands a JSON string corresponding to a single annotated annotation axiom that is one of:
        definition, obo synonyms type (has_exact_synonym etc) into a set of annotation edges to pubs
        in xref annotations. Sets the following edge values:

        value -> value
        scope -> type (if synonym)
        synonym_type -> has_synonym_type
        typ -> def or syn
        """
        supported_types = ['definition']
        synonym_types = ['has_exact_synonym', 'has_narrow_synonym',
                         'has_related_synonym', 'has_broad_synonym']
        supported_types.extend(synonym_types)

        ### Some input tests

        try:
            j = json.loads(json_string)
        except ValueError:
            warnings.warn("Expected JSON string, got '%s'" % str(json_string))
        try:
            assert(type in supported_types)
        except ValueError:
            warnings.warn("Unsupported type '%s'" % str(type))

        try:
            assert("value" in j.keys())
        except ValueError:
            warnings.warn("JSON has no value key. Not loading")
            # might be better to make this a soft warning

        # Start building edge annotations
        edge_annotations = {}  #{"value":  escape_string(j['value'])} # switched to only put value on syn edges.

        ### Set typ (def or syn) + scope for syn
        if type in synonym_types:
            edge_annotations['typ'] = 'syn'
            edge_annotations['scope'] = type
            edge_annotations['value'] = escape_string(j['value'])
        else:
            edge_annotations['typ'] = 'def'
            self.node_writer.statements.append('MATCH (c:Class {short_form: "%s"}) SET c.description = "%s"'
                                             "" % (subject, escape_string(j['value'])))

        # If no xrefs, add a fake one to FlyBase


        if not ('annotations' in j.keys()):
            j['annotations'] = {"database_cross_reference": ['FlyBase:Unattributed']}


        # Loop over annotations, rolling edges to FlyBase pubs

        if 'annotations' in j.keys():
            a = j['annotations']

            # add synonym category if present
            if 'has_synonym_type' in a.keys():
                edge_annotations['has_synonym_type'] = a['has_synonym_type']  # This is category! e.g. plural, not scope

            # process xrefs:
            xrefs_proc = []
            if "database_cross_reference" in a.keys():
                xrefs_proc = [{'db': xref.split(':')[0], 'acc':  xref.split(':')[1]}
                                for xref in a["database_cross_reference"]
                                if xref.split(':')[0] in supported_xrefs]

            # If no supported xrefs are present, add a fake one to FB:Unattributed
            if not xrefs_proc:
                xrefs_proc = [{'db': 'FlyBase', 'acc': 'Unattributed'}]
            # Loop over xrefs adding edges to pub
            i = 0  # for break in test mode
            for x in xrefs_proc:
                # This won't work for unattributed !
                if x['db'] and x['acc'] and x['db'] == 'FlyBase':
                    self.node_writer.add_node(labels=['pub', 'Individual', 'Entity'], IRI=map_iri(x['db']) + x['acc'])
                    self.edge_witer.add_annotation_axiom(s=subject,
                                                         r='references',
                                                         o=x['acc'],
                                                         stype=':Class',
                                                         edge_annotations=edge_annotations,
                                                         match_on='short_form',
                                                         safe_label_edge=True)
                i += 1
                if self.test_mode and i > 99:
                    break

    def gen_pub_links(self):
        q = ["MATCH (c:Class) "  # do we need inds?
             "WHERE exists(c.definition)"
             "OR exists(c.has_exact_synonym) "  
             "OR exists(c.has_broad_synonym) "  
             "OR exists(c.has_narrow_synonym) "  
             "OR exists(c.has_related_synonym) "
            "RETURN c.short_form AS short_form, "
            "{ definition: COALESCE(c.definition, []), "
            "has_exact_synonym: COALESCE(c.has_exact_synonym, []), "
            "has_broad_synonym: COALESCE(c.has_broad_synonym, []), "
            "has_narrow_synonym: COALESCE(c.has_narrow_synonym, []), "
            "has_related_synonym: COALESCE(c.has_related_synonym, [])} "
             "AS annotated_axioms"]
        r = self.node_writer.nc.commit_list(q)
        dc = results_2_dict_list(r)
        for d in dc:
            for k, v in d['annotated_axioms'].items():
                if v:
                    if k == 'definition':
                        self.write_pub_link(d['short_form'], v[0], type=k)
                    else:
                        for s in v:
                            self.write_pub_link(d['short_form'], s, type=k)

    def commit(self):
        self.node_writer.commit(chunk_length=1000, verbose=True)
        self.edge_witer.commit(chunk_length=1000, verbose=True)

def __main__():
    pl = pubLink(endpoint=args.endpoint,
                 usr=args.usr,
                 pwd=args.pwd,
                 test_mode=args.test)
    pl.gen_pub_links()
    pl.commit()

__main__()