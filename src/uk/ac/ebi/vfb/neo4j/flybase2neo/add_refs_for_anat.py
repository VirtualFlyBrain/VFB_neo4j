from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, results_2_dict_list
from uk.ac.ebi.vfb.curie_tools import map_iri
import sys
import json
import re

nc = neo4j_connect(base_uri=sys.argv[1],
                   usr=sys.argv[2],
                   pwd=sys.argv[3])

"""
Converts references on definitions and synonyms, stored as entity attributes
in OLS Neo4j, into edges linked to pub nodes.  In the case of synonyms, 
edges store synonym names, scopes and types. 

Background: 

OLS Neo4J includes references attached to definitions and synonyms, 
but these are packed into JSON strings on attributes.

Almost every reference has an FBrf, but a few only have PMIDS or DOIs. 
Merge strategy uses FBrf first, then PMID, then DOI."""

supported_xrefs = {'FlyBase': 'FlyBase:FBrf\d{7}',
                   'PMID': 'PMID:\d+',
                   'DOI': 'DOI:.+'}


# obo_definition_citation:{"definition":"Any sense organ (FBbt:00005155) that is part of some adult (FBbt:00003004).",
# "oboXrefs":[{"database":"FlyBase","id":"FBrf0031004","description":null,"url":null},
# {"database":"FlyBase","id":"FBrf0007734","description":null,"url":null},
# {"database":"FBC","id":"auto_generated_definition","description":null,"url":null}]}


# obo_synonym:{"name":"IDFP",
#              "scope":"hasBroadSynonym",
#               "type":null,"xrefs":[
#               {"database":"FlyBase","id":"FBrf0212704",
#               "description":null,"url":null}]}
# ,{"name":"vmpr","scope":"hasRelatedSynonym","type":null,"xrefs":[{"database":"FlyBase","id":"FBrf0193607","description":null,"url":null}]},{"name":"LAL","scope":"hasExactSynonym","type":"BrainName official abbreviation","xrefs":[{"database":"FlyBase","id":"FBrf0224194","description":null,"url":null}]}


def proc_xrefs(dbxrefs):
    if not dbxrefs:
        return False
    out = {}
    for db in supported_xrefs.keys():
        out[db] = []
    for xref in dbxrefs:
        for db, re_string in supported_xrefs.items():
            m = re.compile(re_string)
            if re.match(m, xref):
                ref = xref.split(':')[1]
                out[db].append(ref)
    return out


def clean_pub_id_typ(sfid, pub_id_typ):
    pub_id_typ = str(pub_id_typ).replace('.', '_')  # replace invalid dots from DB types such as 'answers.com'
    pub_id_typ = pub_id_typ.replace(' ', '_')  # replace invalid spaces
    if '[' in pub_id_typ:
        print("\n\nNote: Invalid database %s referenced by %s\n\n" % (pub_id_typ, sfid))
        pub_id_typ = pub_id_typ.replace('[', '')  # remove random '['
    return pub_id_typ


def roll_cypher_add_def_pub_link(sfid, pub_id_typ, pub_id):
    """Generates a Cypher statement that links an existing class
    to a pub node with the specified attribute.  Generates a new pub node
     if none exists."""
    return "MATCH (a:Class { short_form : \"%s\" }) " \
           "MERGE (p:pub:Individual { short_form : \"%s\" }) " \
           "MERGE (a)-[:has_reference { typ : \"def\" }]->(p)" % (sfid, pub_id)


def roll_cypher_add_syn_pub_link(sfid, s, pub_id_typ, pub_id):
    """Generates a Cypher statement that links an existing class
    to a pub node ..."""
    label = re.sub("'", "\'", s['name'])
    return "MATCH (a:Class { short_form : \"%s\" }) " \
           "MERGE (p:pub:Individual { short_form : \"%s\" }) " \
           "MERGE (a)-[:has_reference { typ : \"syn\", scope: \"%s\", " \
           "synonym : \"%s\", cat: \"%s\" }]->(p)" \
           "" % (sfid, pub_id, s['scope'], label, s['type'])


nc.commit_list(["MERGE (:pub:Individual { short_form: 'Unattributed' })"])
q = nc.commit_list(["MATCH (c) where c:Class or c:Individual "
                    "RETURN c.short_form as short_form, "
                    "c.synonym as slist, c.obo_synonym as refd_syns,"
                    " c.obo_definition_citation as def"])
dc = results_2_dict_list(q)
statements = []
for d in dc:
    if d['def']:
        for cit in d['def']:
            if cit:
                def_cit = json.loads(cit)
                for ref in def_cit['oboXrefs']:
                    if ref['id'] and ref['database'] == 'FlyBase':
                        statements.append(roll_cypher_add_def_pub_link(
                            sfid=d['short_form'],
                            pub_id=ref['id'],
                            pub_id_typ=ref['database'],
                        ))
    # slist = simple list of strings containing all all synonyms
    # refd_synse = list of JSON strings for all_synonyms with xrefs (may be subset of slist)
    # generate a dict of in refd syns keyd on name
    # iterate of slist - checking if ref'd syn dict keys
    if d['slist']:
        if d['refd_syns']:
            rsd = {x['name']: x for x in [json.loads(y) for y in d['refd_syns']]}
        else:
            rsd = {}
        for syn in d['slist']:
            if syn in rsd.keys():
                rs = rsd[syn]
            else:
                rs = {'name': syn,
                      'scope': 'hasExactSynonym',
                      'type': '',
                      'xrefs': [{'id': 'Unattributed', 'database': 'FlyBase'}]}  #
            for ref in rs['xrefs']:
                if ref['database'] == 'FlyBase':
                    statements.append(roll_cypher_add_syn_pub_link(
                        sfid=d['short_form'],
                        pub_id=ref['id'],
                        pub_id_typ=ref['database'],
                        s=rs))
nc.commit_list_in_chunks(statements, verbose=True, chunk_length=2000)

# to json
# proc to new commit statements.


# tests
# obo_synonym_string = '[{"name":"IDFP","scope":"hasBroadSynonym","type":null,"xrefs":[{"database":"FlyBase","id":"FBrf0212704","description":null,"url":null}]},{"name":"vmpr","scope":"hasRelatedSynonym","type":null,"xrefs":[{"database":"FlyBase","id":"FBrf0193607","description":null,"url":null}]},{"name":"LAL","scope":"hasExactSynonym","type":"BrainName official abbreviation","xrefs":[{"database":"FlyBase","id":"FBrf0224194","description":null,"url":null}]}]'
# obo_synonym = json.loads(obo_synonym_string)
# obo_definition_citation_string = '{"definition":"A sense organ embedded in the integument and consisting of one or a cluster of sensory neurons and associated sensory structures, support cells and glial cells forming a single organized unit with a largely bona fide boundary.","oboXrefs":[{"database":"FlyBase","id":"FBrf0111704","description":null,"url":null}]}'
# obo_definition_citation = json.load(obo_definition_citation_string)
q