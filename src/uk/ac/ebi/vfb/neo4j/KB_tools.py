'''
Created on Mar 6, 2017

@author: davidos
'''
import logging
import warnings
import re
import json
import pandas as pd
import http.client
#import psycopg2
import requests
from requests.exceptions import ChunkedEncodingError
from .neo4j_tools import neo4j_connect, results_2_dict_list
from .SQL_tools import get_fb_conn, dict_cursor
from ..curie_tools import map_iri
import base36
from better_profanity import profanity


#  * OWL - Only edges of types Related, INSTANCEOF, SUBCLASSOF are exported to OWL.
#    * (:Individual)-[:Related { iri: '', label: ''}]-(:Individual)  -> OWL FACT (OPA)
#    * (:Individual)-[:Related { iri: '', label: ''}]-(:Class) -> OWL Type: R some C
#    * (:Class)-[:Related { iri: '', label: ''}]-(:Individual) -> OWL SubClassOf: R value I
#    * (:Class|Individual]-[:Annotation { iri: '' ...}-[:Individual]

# But really - all these should be flipped => edges with readable names current type as attributes type = ...

# Match statements checks for all relevant entites, including relations if applicable. Implementing methods should 
# check return values and warn/fail as appropriate if no match.

# TODO: Add lookup for attributes -> Properties.  Ideally this would be with a specific cypher label for APs.
# May want to follow a prefixed pattern to indicate OWL compatible APs.


# Sketch of separating out property lookup:

## - property lookup queries need to be pushed to a different stack.
## - Committed before edge writer queries.
## - Edge writer queries need to be re-written based on the output of this
# - so they need to live on a separate stack too and only get pushed to statements
# once substitution has happened.

def get_sf(iri):
    """Get a short form from an iri."""
    return re.split('[#/]', iri)[-1]

def gen_id(idp, ID, length, id_name, use_base36=False):
    """
    Generates an ID of form <idp>_<padded_accession>
    ARG1: idp (string), 
    ARG 2 starting ID number (int), 
    ARG3, length of numeric portion ID, 
    ARG4 an id:name hash"""
    def gen_key(ID, length):  # This function is limited to the scope of the gen_id function.
        dl = len(str(ID))  # coerce int to string.
        k = idp+'_'+(length - dl)*'0'+str(ID)
        return k

    k = gen_key(ID, length)

    while k in id_name:
        if use_base36:
            ID = base36.dumps(base36.loads(ID) + 1)
        else:
            ID += 1
        k = gen_key(ID, length)

    return {'short_form': k, 'acc_int': ID}  # useful to return ID to use for next round.


def contains_profanity(value, use_base36):
    """
    Checks if the input text has any swear words.
    Parameters:
        value: phrase to check
        use_base36: boolean variable to enable/disable base36 validation
    Return:
        True if the input text has any swear words, False otherwise.
    """
    if use_base36:
        min_length = 3
        max_length = len(value)

        all_substrings = [value[i:i + j] for i in range(len(value) - min_length) for j in
                          range(min_length, max_length + 1)]
        return profanity.contains_profanity(" ".join(all_substrings))
    return False



class kb_writer (object):
      
    def __init__(self, endpoint, usr, pwd, hard_fail=False):
        self.nc = neo4j_connect(endpoint, usr, pwd)
        self.endpoint = endpoint
        self.usr = usr 
        self.pwd = pwd
        self.statements = []
        self.output = []
        self.log = []

    def _commit(self, verbose=False, chunk_length=5000, max_retries=5, delay=5):
        """Commits Cypher statements stored in object.
        Flushes existing statement list.
        Returns REST API output.
        Optionally set verbosity and chunk length for commits.
        Retries the commit in case of RemoteDisconnected errors."""
        retries = 0
        while retries < max_retries:
            try:
                self.output = self.nc.commit_list_in_chunks(
                    statements=self.statements,
                    verbose=verbose,
                    chunk_length=chunk_length)
                self.statements = []
                return self.output
            except (http.client.RemoteDisconnected, ChunkedEncodingError, ConnectionResetError) as e:
                retries += 1
                if retries >= max_retries:
                    raise e
                print(f"RemoteDisconnected encountered. Retrying {retries}/{max_retries} in {delay} seconds...")
                time.sleep(delay)
                self.nc = neo4j_connect(self.endpoint, self.usr, self.pwd)  # Re-establish the connection

    def commit(self, verbose=False, chunk_length=5000):
        return self._commit(verbose, chunk_length)

    def escape_string(self, strng:str):
        # backslashes need special escaping to be treated literally
        return re.sub(r'\\', r'\\\\', strng)
  
    def _add_textual_attribute(self, var, key, value):
        return 'SET %s.%s = "%s" ' % (var, key, self.escape_string(value)) # Note arrangement single and double quotes
    
    def _set_attributes_from_dict(self, var, attribute_dict):
        """Generates CYPHER `SET` sub-clauses 
        from key value pairs in a dict (attribute_dict).
        Values must be int, float, string or list.
        var = variable name in CYPHER statement.
        """
        # Note - may be able to simplify this by converting to a map and passing that.
        out = ''
        for k,v in attribute_dict.items():
            if type(v) == int:
                out += "SET %s.%s = %d " % (var,k,v)
            elif type(v) == float:   
                out += "SET %s.%s = %f " % (var,k,v)                    
            elif type(v) == str:
                out += 'SET %s.%s = "%s" ' % (var, k, self.escape_string(v))           
            elif type(v) == list:                        
                out += 'SET %s.%s = %s ' % (var,k, str([self.escape_string(i) for i in v]))
            elif type(v) == bool:
                out += "SET %s.%s = %s " % (var,k, str(v))                
            else: 
                warnings.warn("Can't use a %s as an attribute value in Cypher. Key %s Value :%s" 
                              % (type(v), k, (str(v))))
        return out

class iri_generator(kb_writer):
    """
    A wrapper class for generating IRIs for *OWL individuals* that don't stomp on those already in the KB.
    """
    def __init__(self, endpoint, usr, pwd,
                 use_base36=False,
                 idp='VFB',
                 acc_length=8,
                 base=map_iri('vfb'),
                 start=0):
        super().__init__(endpoint, usr, pwd)
        self.use_base36 = use_base36
        self._configure(idp=idp, acc_length=acc_length, base=base)

    def _configure(self, idp, acc_length, base):
        self.acc_length = acc_length
        self.idp = idp
        self.id_name = {}
        self.base = base
        self.lookup = set()
        self.statements.append("MATCH (i:Individual) "
                               "WHERE i.short_form =~ '%s_[0-9a-z]{%d}' "  # Note POSIX regex rqd
                               "RETURN i.short_form as short_form, "
                               "i.label as label" % (idp, acc_length))
        r = self.commit()
        if r:
            results = results_2_dict_list(r)
            for res in results:
                self.id_name[res['short_form']] = res['label']
                acc = res['short_form'].split('_')[1]
                if not self.use_base36:
                    try:
                        self.lookup.add(int(acc))
                    except:
                        self.lookup.add(base36.loads(acc))
                else:
                    self.lookup.add(base36.loads(acc))
            return True
        else:
            warnings.warn("No existing ids match the pattern %s_%s" % (idp, 'n'*acc_length))
            return False

    def set_channel_config(self):
        self._configure(idp='VFBc', acc_length=8, base=map_iri('vfb'))

    def generate(self, start, label=''):
        acc = self._get_new_accession(start)
        short_form = self._gen_short_form(acc)
        if self.idp == 'VFB':
            channel_short_form = self._gen_channel_short_form(acc)
            iri = self.base + short_form
            channel_iri = self.base + channel_short_form

            while short_form in self.id_name or channel_short_form in self.id_name:
                if self.use_base36:
                    acc = base36.dumps(base36.loads(acc) + 1)
                else:
                    acc += 1
                short_form = self._gen_short_form(acc)
                channel_short_form = self._gen_channel_short_form(acc)
                iri = self.base + short_form
                channel_iri = self.base + channel_short_form

            self.id_name[short_form] = label
            self.id_name[channel_short_form] = label + '_c'
            return {'iri': iri, 'short_form': short_form, 'channel_iri': channel_iri, 'channel_short_form': channel_short_form}
        else:
            iri = self.base + short_form
            while short_form in self.id_name:
                if self.use_base36:
                    acc = base36.dumps(base36.loads(acc) + 1)
                else:
                    acc += 1
                short_form = self._gen_short_form(acc)
                iri = self.base + short_form
            self.id_name[short_form] = label
            return {'iri': iri, 'short_form': short_form}

    def _get_new_accession(self, start):
        if self.use_base36:
            i = base36.loads(start)
        else:
            i = int(start)  # casting just in case
        while i in self.lookup or contains_profanity(base36.dumps(i), self.use_base36):
            i += 1
        self.lookup.add(i)
        if self.use_base36:
            return base36.dumps(i)
        else:
            return i

    def _gen_short_form(self, accession):
        return self.idp + '_' + str(accession).zfill(self.acc_length)

    def _gen_channel_short_form(self, accession):
        return 'VFBc_' + str(accession).zfill(self.acc_length)

class kb_owl_edge_writer(kb_writer):
    """A class wrapping methods for updating imported entities in the KB.
    Constructor: kb_owl_edge_writer(endpoint, usr, pwd)
    """

    def __init__(self, endpoint, usr, pwd, hard_fail=False):
        self.nc = neo4j_connect(endpoint, usr, pwd)
        self.statements = []
        self.output = []
        # An objecty representation of properties might be more easily maintainable.
        self.properties = {}  # Dict of properties.
        self.triples = {}  # Dict of lists of args to a triples method, keyed on op.
        self.hard_fail = hard_fail
        self.log = []

    def check_properties(self):
        """Check whether properties used in triples have a corresponding Property node.
        Lookup is via property specified in match_on arg in an add_triple method.
        Purge triples using properties not found in the DB from the stack."""

        statements = []
        for k, v in self.properties.items():
            # If this was only operating on Neo3, could just grab all node properties as a map.
            statements.append(
                "OPTIONAL MATCH (r:Property { %s: '%s' }) " 
                "RETURN r.iri as iri, r.short_form as short_form, " 
                "r.label as label, '%s' as key, '%s' as match_on" % (
                    v['match_on'], k, k, v['match_on'])
            )
        if not statements:
            warnings.warn("No properties to check.")
            return
        q = self.nc.commit_list(statements)
        dc = results_2_dict_list(q)

        for d in dc:
            if d[d['match_on']]:
                self.properties[d['key']].update({'iri': d['iri'],
                                                  'short_form': d['short_form'],
                                                  'label': d['label']})
                if not d['iri']:
                    m = '%s in KB but has no iri!' % d['key']
                    warnings.warn(m)
                    self.log.append(m)
                # Add checks for short_form and label?
            else:
                w = "Not in KB! %s" % d['key']
                if self.hard_fail:
                    raise ValueError(w)
                else:
                    self._report_missing_property(d['key'])

    def _report_missing_property(self, prop):
        """Remove triples using specified prop and warn."""
        for t in self.triples.pop(prop):
            m = "Unknown property %s: Can't add triple %s, %s, %s." % (prop,
                                                                              t['o'],
                                                                              prop,
                                                                              t['s'])
            self.log.append(m)
            warnings.warn(m)

    def _add_triple(self, s, r, o, rtype, stype, otype,
                    edge_annotations=None, match_on="iri", safe_label_edge=True):
        # Private method to set up data structures required for checking properties
        # prior to constructing cypher specifying triples for addition.

        if edge_annotations is None:
            edge_annotations = {}
        if match_on not in ['iri', 'label', 'short_form']:
            raise Exception("Illegal match property '%s'. " \
                            "Allowed match properties are 'iri', 'label', 'short_form'" % match_on)
        if r in self.triples.keys():
            self.triples[r].append(locals())
        else:
            self.triples[r] = [locals()]
        # built on the assumption that match_on is always a proxy for o type (!)
        if r not in self.properties.keys():
            self.properties[r] = {'match_on': match_on}

    def _construct_triples(self):
        # Private method to construct triples once properties have been checked.

        flat_list_triples = [item for sublist in self.triples.values() for item in sublist]
        for t in flat_list_triples:
            rel_map = self.properties[t['r']]
            if t['safe_label_edge']:
                if 'sl' in rel_map.keys() and rel_map['sl']:
                    rel = rel_map['sl']
                elif 'label' in rel_map.keys() and rel_map['label']:
                    rel = re.sub('\W', '_', rel_map['label'])
                else:
                    rel = rel_map['short_form']
            else:
                rel = rel_map[t['match_on']]

            out = "OPTIONAL MATCH (s%s { %s:'%s' }) " % (t['stype'],t['match_on'], t['s'])
            out += "OPTIONAL MATCH (o%s { %s:'%s' }) " % (t['otype'], t['match_on'], t['o'])
            out += "FOREACH (a IN CASE WHEN s IS NOT NULL THEN [s] ELSE [] END | " \
                   "FOREACH (b IN CASE WHEN o IS NOT NULL THEN [o] ELSE [] END | "
            if t['safe_label_edge']:
                out += "MERGE (a)-[re:%s]->(b) SET re.type = '%s' " % (rel, t['rtype'])  # Might need work?
            else:
                out += "MERGE (a)-[re:%s { %s: '%s' }]->(b) " % (t['rtype'],
                                                                t['match_on'],
                                                                rel)
            out += self._set_attributes_from_dict('re', t['edge_annotations'])

            # For each of label, iri, short_form; Check if available; Check if used in match
            # (and therefore in edge merge) or if edge is typed as safe label.
            # This is needed as cypher doesn't like property used in merge is also set in same statement.
            if rel_map['label'] and ((not t['match_on'] == 'label') or t['safe_label_edge']):
                out += "SET re.label = '%s' " % rel_map['label']
            if rel_map['short_form'] and ((not t['match_on'] == 'short_form' ) or t['safe_label_edge']):
                out += "SET re.short_form = '%s' " % rel_map['short_form']
            if rel_map['iri'] and ((not t['match_on'] == 'iri') or t['safe_label_edge']):
                out += "SET re.iri = '%s' " % rel_map['iri']
            out += ")) RETURN { `%s`: count(s), `%s`: count(o) } as match_count" % (t['s'], t['o'])
            self.statements.append(out)


    def _add_related_edge(self, s, r, o, stype, otype,
                          edge_annotations=None, match_on="iri", safe_label_edge=True):
        if edge_annotations is None:
            edge_annotations = {}
        rtype = 'Related'
        self._add_triple(s, r, o, rtype, stype, otype,
                         edge_annotations, match_on, safe_label_edge=safe_label_edge)

    def add_annotation_axiom(self, s, r, o, stype='', otype=':Individual', edge_annotations=None, match_on="iri", safe_label_edge=True):
        """Link an OWL entity to an Individual via an annotation axiom.
        s = property identifying subject entity ,
        r = property identifying relation (AnnotationProperty) ,
        o = property identifying object individual.
        match_on = property to match individuals/Property on; default = 'iri'
        Optionally add edge annotations specified as key value pairs in dict.
        Optionally specify edge type as safe_label (default = False => edge type: Annotation)
       """
        if edge_annotations is None:
            edge_annotations = {}
        rtype = 'Annotation'
        self._add_triple(s, r, o, rtype, stype, otype,
                         edge_annotations, match_on, safe_label_edge=safe_label_edge)

    def add_fact(self, s, r, o, edge_annotations=None,
                 match_on="iri", safe_label_edge=True):

        """Add OWL fact to statement stack.
        s = property identifying subject individual ,
        r = property identifying relation (ObjectProperty) ,
        o = property identifying object individual.
        match_on = property to match individuals/Property on; default = 'iri'
        Optionally add edge annotations specified as key value pairs in dict.
        Optionally specify edge type as safe_label (default = False => edge type: Related)
       """
        if edge_annotations is None: edge_annotations = {}
        self._add_related_edge(s, r, o, stype=":Individual", otype=":Individual",
                               edge_annotations=edge_annotations,
                               match_on=match_on,
                               safe_label_edge=safe_label_edge)

    def add_xref(self, s, xref, stype=''):
        """Add an xref axiom"""
        # Add regex test for xref
        x = xref.split(':')
        self.add_annotation_axiom(s=s,
                                  r='hasDbXref',
                                  o=x[0],
                                  otype=':Individual',
                                  stype=stype,
                                  edge_annotations={'accession': [x[1]]},
                                  match_on='short_form',
                                  safe_label_edge=True)

                
    def add_anon_type_ax(self, s, r, o, edge_annotations=None,
                         match_on="iri", safe_label_edge=True):
        """Add OWL anonymous type axiom to statement stack.
        s = property identifying subject individual ,
        r = property identifying relation (ObjectProperty) ,
        o = property identifying object class.
        match_on = property to match owl entities on; default = 'iri'
        Optionally add edge annotations specified as key value pairs in dict.
        Optionally specify edge type as safe_label (default = False => edge type: Related)
       """
        if edge_annotations is None: edge_annotations = {}
        self._add_related_edge(s, r, o, stype=":Individual", otype=":Class",
                               edge_annotations = edge_annotations, 
                               match_on = match_on,
                               safe_label_edge=safe_label_edge)

    def add_named_type_ax(self, s, o, match_on="iri", edge_annotations=None):
        """Add OWL named type axiom to statement stack.
         s = property identifying subject Individual ,
         o = property identifying object Class.
         match_on = property to match owl entities on; default = 'iri'
         Optionally add edge annotations specified as key value pairs in dict."""

        if edge_annotations is None: edge_annotations = {}
        out = "OPTIONAL MATCH (s:Individual {{ {match_on}:'{s}' }} ) " \
              "OPTIONAL MATCH (o:Class {{ {match_on}:'{o}' }} ) ".format(**locals())
        out += "FOREACH (a IN CASE WHEN s IS NOT NULL THEN [s] ELSE [] END | " \
               "FOREACH (b IN CASE WHEN o IS NOT NULL THEN [o] ELSE [] END | " \
               "MERGE (a)-[i:INSTANCEOF]->(b) "
        if edge_annotations:
            out += self._set_attributes_from_dict('i', edge_annotations)
        out += ")) RETURN { `%s`: count(s), `%s`: count(o) } as match_count" % (s, o)
        self.statements.append(out)

    def add_anon_subClassOf_ax(self, s, r, o, edge_annotations=None,
                               match_on="iri", safe_label_edge=True):
        """Add OWL anonymous subClassOf axiom to statement stack.
        s = property identifying subject Class ,
        r = property identifying relation (ObjectProperty) ,
        o = property identifying object Class.
        match_on = property to match owl entities on; default = 'iri'
        Optionally add edge annotations specified as key value pairs in dict.
        Optionally specify edge type as safe_label (default = False => edge type: Related)
        """

        if edge_annotations is None: edge_annotations = {}
        self._add_related_edge(s, r, o, stype = ":Class", otype = ":Class",
                               edge_annotations = edge_annotations,
                               match_on = match_on,
                               safe_label_edge=safe_label_edge)

    def add_named_subClassOf_ax(self, s, o, match_on="iri"):
        """Add OWL named type axiom to statement stack.
            s = property identifying subject Individual ,
            o = property identifying object Class.
            match_on = property to match owl entities on; default = 'iri'"""
        out = "OPTIONAL MATCH (s:Class {{ {match_on}:'{s}' }} ) " \
              "OPTIONAL MATCH (o:Class {{ {match_on}:'{o}' }} ) ".format(**locals())
        out += "FOREACH (a IN CASE WHEN s IS NOT NULL THEN [s] ELSE [] END | " \
               "FOREACH (b IN CASE WHEN o IS NOT NULL THEN [o] ELSE [] END | " \
               "MERGE (a)-[:SUBCLASSOF]->(b) "
        out += ")) RETURN { `%s`: count(s), `%s`: count(o) } as match_count" % (s, o)
        self.statements.append(out)
    
    def commit(self, verbose=False, chunk_length=5000):
        """Check prroperties; construct triples for all properties present;
        commit all edge additions (triples and duples) and test success.
        Reset stacks to zero. Return any output from commit.
        Optional args: set verbosity number of statements per commit (chunk_length)/"""

        self.check_properties()
        self._construct_triples()
        self._commit(verbose, chunk_length)
        self.test_edge_addition() # Do something with return value?
        # At this point - resetting all attributes except connection to default.
        # Better practice to just make a new object?
        out = self.output
        self.statements = []
        self.output = []
        self.log = []
        self.properties = {} # Dict of properties
        self.triples = {} # Dict of lists of triples keyed on property
        return out

    def test_edge_addition(self):
        """Tests lists of return values from REST API for edge creation
        """
        dc = results_2_dict_list(self.output)
        missed_edges = [x['match_count'] for x in dc if x and (0 in x['match_count'].values())]
        if missed_edges:
            for e in missed_edges:
                m = "No match found for %s" % str([k for k, v in e.items() if not v])
                self.log.append(m)
                warnings.warn(m)
            return False
        else:
            return True

class node_importer(kb_writer):
    """A class wrapping methods for updating imported entities in the KB,
    e.g. from ontologies, FlyBase, CATMAID.
    Constructor: owl_import_updater(endpoint, usr, pwd)
    """
        
    def add_constraints(self, uniqs=None, indexes=None):
        """Specify addition uniqs and indexes via dicts.
        { label : [attributes] } """
        if uniqs is None: uniqs = {}
        if indexes is None: indexes = {}
        for k,v in uniqs.items():
            for a in v:
                self.statements.append("CREATE CONSTRAINT ON (n:%s) ASSERT n.%s IS UNIQUE" % (k,a))
        for k,v in indexes.items():
            for a in v:
                self.statements.append("CREATE INDEX ON :%s(%s)" % (k,a))
            
    def add_default_constraint_set(self, labels):
        """SETS iri and short_form as uniq, indexes label"""
        uniqs = {}
        indexes = {}
        for l in labels:
            uniqs[l] = ['iri', 'short_form']
            indexes[l] = ['label']
        self.add_constraints(uniqs, indexes)
        self.commit()
            
    def add_node(self, labels, IRI, attribute_dict=None, allow_duplicates=False):
        """Adds or updates a node.
        Node uniqueness specified by IRI + labels.
        Derives short_form using has or / as delimiter
        Adds/Updates attributes to those specified in the attribute dict
        """
        if attribute_dict is None: attribute_dict = {}
        short_form = re.split('[#/]', IRI)[-1]
        logging.debug(f"Adding node with labels: {labels}, IRI: {IRI}, short_form: {short_form}, attributes: {attribute_dict}, allow_duplicates: {allow_duplicates}")
        statement = "MERGE (n:%s { iri: '%s' }) set n.short_form = '%s' set n:Entity " % (
                    (':'.join(labels)),
                     IRI, short_form)
        statement += self._set_attributes_from_dict(var='n',
                                                    attribute_dict=attribute_dict)
        self.statements.append(statement)
        logging.debug(f"Generated statement: {statement}")
    
    def update_from_obograph(self, file_path = '', url = '', include_properties=False, commit=True):
        """Update property and class nodes from an OBOgraph file
        (currently does not distinguish OPs from APs!)
        Only updates from primary graph (i.e. ignores imports)
        """
        ## Get JSON, assuming only primary graph should be used for updating
        ## ie: imports ignored.

        def obs_check(nod):
            obs_stat = False
            if 'meta' in nod.keys():
                if 'deprecated' in nod['meta'].keys():
                    obs_stat = nod['meta']['deprecated']
                elif 'basicPropertyValues' in nod['meta'].keys():
                    x = [ax['val'] for ax in nod['meta']['basicPropertyValues']
                         if ax['pred'] == 'http://www.w3.org/2002/07/owl#deprecated']
                    if x:
                        obs_stat = x[0]
            if obs_stat is True or obs_stat == 'true':
                return True
            else:
                return False

        if file_path:   
            f = open(file_path, 'r')
            obographs = json.loads(f.read())
            f.close()
            primary_graph = obographs['graphs'][0]
        elif url:
            r = requests.get(url)
            if r.status_code == 200:
                obographs = r.json()
                primary_graph = obographs['graphs'][0]   # Add a check for success here!
            else:
                warnings.warn("URL connection issue %s %s for %s" % (r.status_code, 
                                                              r.reason, url))
                return False
        else:
            warnings.warn('Please provide a file_path or a URL')
            return False

        for node in primary_graph['nodes']:
            IRI = node['id']
            attribute_dict = {}
            if 'type' in node.keys():
                if node['type'] == 'CLASS':
                    labels = ['Class']
                elif node['type'] == 'PROPERTY' and include_properties:
                    labels = ['Property']
                else:
                    continue
            # Split URL -> base & short_form
            m = re.findall('.+(#|/)(.+?)$', node['id'])
            attribute_dict['short_form'] =  m[0][1]
            if 'lbl' in node.keys(): attribute_dict['label']=  node['lbl']
            if 'meta' in node.keys():
                if obs_check(node):
                    attribute_dict['is_obsolete'] = obs_check(node)
            ## Update nodes.
            self.add_node(labels, IRI, attribute_dict)
        if commit:
            self.commit()
            obsolete_terms = self.check_for_obsolete_nodes_in_use()
            if obsolete_terms:
                self.merge_obsoletes(obsolete_terms, primary_graph)
        return True

    def check_for_obsolete_nodes_in_use(self):
        m = "MATCH (c:Class)-[r]-(fu) WHERE c.is_obsolete=True " \
            "RETURN c.label, c.iri"
        q = results_2_dict_list(self.nc.commit_list([m]))
        if q:
            for r in q:
                warnings.warn("%s, %s is obsolete but in use." % 
                              (r['c.label'], r['c.iri']))
            obsolete_iris = [r['c.iri'] for r in q]
            return list(set(obsolete_iris))
        else:
            print("No obsolete nodes in use.")
            return False

    def term_replacement_command_writer(self, old_id, new_id):
        """
        Returns cypher commands for merging old_id (IRI) and new_id (short) in KB.
        """
        # get all relationships (need to specify each one in a separate command)
        q = "match ()-[r]->() return distinct TYPE(r) AS rel_types"
        d = results_2_dict_list(self.nc.commit_list([q]))
        rel_types = pd.DataFrame.from_dict(data=d)

        # make a command to replace usage for every relationship type
        commands = []
        for t in rel_types['rel_types']:
            new_command = ("MATCH (c:Class {iri: '%s'})<-[r:%s]-(i:Individual), "
                           "(c2:Class {short_form: '%s'}) MERGE (c2)<-[r2:%s]-(i) "
                           "SET r2=properties(r) DELETE r") % (old_id, t, new_id, t)
            commands.append(new_command)

        return commands

    def merge_obsoletes(self, ob_term_ids, graph):
        """
        Maps a list of full length IRIs to their 'term replaced by', where possible.
        Produces cypher commands to transfer annotations in VFB to the replacement terms.
        """
        def convert_to_short_form(iri):
            """
            Convert any (: or _) id to a short form.
            Returns input if ID doesn't match pattern.
            """
            pattern = re.compile("([A-Za-z]+)[_:]([0-9]+)$")
            m = re.search(pattern, iri)

            if m:
                short_form = m.group(1) + '_' + m.group(2)
                return short_form
            else:
                return iri

        # get mappings based on term_replaced_by (IAO_0100001)
        mapping_dict = {}
        for i in ob_term_ids:
            for n in graph['nodes']:
                if n['id'] == i:
                    try:
                        for p in n['meta']['basicPropertyValues']:
                            if p['pred'] == "http://purl.obolibrary.org/obo/IAO_0100001":
                                mapping_dict[i] = convert_to_short_form(p['val'])
                    except KeyError:
                        continue

        # add merge commands to statements (to auto-merge) and collect unmapped obsolete terms
        failed_mappings = []
        for i in ob_term_ids:
            try:
                x = self.term_replacement_command_writer(old_id=i, new_id=mapping_dict[i])
                self.statements.extend(x)
            except KeyError:
                failed_mappings.append(convert_to_short_form(i))
                continue
        self.commit()

        # get mappings based on consider annotations for terms with no term_replaced_by
        if len(failed_mappings) > 0:
            failed_mapping_dict = {}
            consider_all_shortids = []
            for i in failed_mappings:
                consider_all_shortids.append(i)
                for n in graph['nodes']:
                    if i in n['id']:
                        try:
                            consider_list = [convert_to_short_form(p['val']) for p in n['meta']['basicPropertyValues'] if
                                             p['pred'] == "http://www.geneontology.org/formats/oboInOwl#consider"]
                            failed_mapping_dict[convert_to_short_form(i)] = consider_list
                            consider_all_shortids.extend(consider_list)
                        except KeyError:
                            failed_mapping_dict[convert_to_short_form(i)] = ["<no suggestions>"]

            # dictionary of short forms and labels for all consider mappings
            consider_label_lookup = {}
            for i in consider_all_shortids:
                consider_label_lookup[i] = "<no label>"
                for n in graph['nodes']:
                    if i in n['id']:
                        try:
                            consider_label_lookup[i] = n['lbl']
                        except KeyError:
                            continue

            # terminal output to show mappings based on consider (no auto-merging)
            print("Warning: Some terms could not be mapped using term_replaced_by:")
            for i in failed_mappings:
                print('  %s (%s):' % (i, consider_label_lookup[i]))
                try:
                    for r in failed_mapping_dict[i]:
                        print('    consider - %s (%s)' % (r, consider_label_lookup[r]))
                except KeyError:
                    print('    <no suggestions>')
            return False

        else:
            print("All obsolete terms mapped successfully")
            return True

    def update_from_flybase(self, load_list):            
            """
            Add feature nodes to KB from FlyBase
            load_list = list of fb feature.uniquename strings.
            """
            
            fbc = get_fb_conn()
            cursor = fbc.cursor()
            
            query = "SELECT f.uniquename, f.name, f.is_obsolete from feature f " \
                    "JOIN cvterm typ on f.type_id = typ.cvterm_id "
            # if load_list:
            load_list_string = "'" + "','".join(load_list) + "'"
            query += "WHERE f.uniquename in (%s) " % load_list_string
#             else:
#                 query += "WHERE typ.name in ('gene', " \
#                 "'transposable_element_insertion_site', 'transgenic_transposon') "
            
            cursor.execute(query)
            dc = dict_cursor(cursor)
            matched = set()
            for d in dc:
                matched.add(d['uniquename'])
                IRI = map_iri('fb') +  d['uniquename']
                attribute_dict = {}
                attribute_dict['label'] = d['name']               
                attribute_dict['short_form'] = d['uniquename']
                attribute_dict['is_obsolete'] = bool(d['is_obsolete'])       
                self.add_node(labels = ['Class', 'Feature'],
                              IRI = IRI,
                              attribute_dict = attribute_dict)
            diff = set(load_list) - matched
            if diff:
                warnings.warn("The following features did not match any known " \
                              " feature in FlyBase: %s" % str(diff))
            cursor.close()
            fbc.close()
            # How to set warning for case where nothing added?
    
    def update_current_features_from_FlyBase(self):
        s = ["MATCH (f:Feature:Class) return f.short_form"]
        r = self.nc.commit_list(s)    
        features = [result['row'][0] for result in r[0]['data']]
        self.update_from_flybase(load_list = features)
        
    def migrate_features_to_new_ids(self, d):
        """STUB"""
        return


class EntityChecker(kb_writer):

    """Check for the existence of nodes or dbxrefs"""

    def __init__(self, endpoint, usr, pwd):
        super(EntityChecker, self).__init__(endpoint, usr, pwd)
        self.should_exist = []
        self.should_not_exist = []
        self.cache = []

    def roll_entity_check(self, labels, query, match_on='short_form'):

        """Roll a check and add it to the stack.
        labels = list of Neo4J labels to match on. You must provide at least one.
        match_on = property to match_on (default = short_form)
        query = Value of property matched on for target entity.
        """
        if query in self.cache:
            return True
        lstring = ':'.join(labels)
        self.should_exist.append("OPTIONAL MATCH (n:%s { %s : '%s'})"
                                 " return n.short_form as result, "
                                 "'%s' as query" % (lstring,
                                                    match_on,
                                                    query,
                                                    query))

    def roll_dbxref_check(self, db, acc):
        if ':'.join([db, str(acc)]) in self.cache:
            return True
        self.should_not_exist.append(
            "OPTIONAL MATCH (s:Individual { short_form: '%s' } )"
            "<-[r:hasDbXref { accession: ['%s'] }]-(i:Individual) "
            "WHERE (s:Site OR s:API) AND exists(s.unique_id) AND s.unique_id=[true] "
            "RETURN s.short_form + ':' + r.accession AS result, "
            "'%s:%s' AS query" % (db, acc, db, acc))

    def roll_new_entity_check(self, labels, query, match_on='short_form', allow_duplicates=False):
        """Roll a check and add it to the stack.
        labels = list of Neo4J labels to match on. You must provide at least one.
        match_on = property to match_on (default = short_form)
        query = Value of property matched on for target entity.
        allow_duplicates = If True, will not check for duplicates in the DB.
        """
        if query in self.cache or allow_duplicates:
            return True
        lstring = ':'.join(labels)
        self.should_not_exist.append("OPTIONAL MATCH (n:%s { %s : '%s'})"
                                 " return n.short_form as result, "
                                 "'%s' as query" % (lstring,
                                                    match_on,
                                                    query,
                                                    query))

    def _check_should_not_exist(self, hard_fail=False):
        self.statements.extend(self.should_not_exist)
        self.should_not_exist.clear()
        return(self._check("Already in DB: ", exists=False, hard_fail=hard_fail))

    def _check_should_exist(self, hard_fail=False):
        self.statements.extend(self.should_exist)
        self.should_exist.clear()
        return self._check("Unknown entity: ", hard_fail=hard_fail)

    def check(self, hard_fail=False):
        a = self._check_should_exist(hard_fail=hard_fail)
        b = self._check_should_not_exist(hard_fail=hard_fail)
        if not (a and b):
            return False
        else:
            return True


    def _check(self, error_message, exists=True, hard_fail=False):
        """Run checks in the stack then empty the stack.
        If hard_fail = True, raise exception if any check in the stack fails."""
        dc = results_2_dict_list(self.commit())
        out = {}
        for d in dc:
            # Everything checked goes in the cache, no matter the result.
            self.cache.append(d['query'])
            if bool(d['result']) is exists:
                out[d['query']] = True
            else:
                out[d['query']] = False
                warnings.warn(error_message + d['query'])
                self.log.append(error_message + d['query'])
        if False in out.values():
            if hard_fail:
                raise Exception(error_message)
            else:
                return False
        else:
            return True
    
class KB_pattern_writer(object):
    """A wrapper class for adding subgraphs following some pre-specified
    schema pattern.
    """
    
    def __init__(self, endpoint, usr, pwd, use_base36=False):
        self.ew = kb_owl_edge_writer(endpoint, usr, pwd)
        self.ni = node_importer(endpoint, usr, pwd)
        self.iri_gen = iri_generator(endpoint, usr, pwd, use_base36=use_base36)
        self.ec = EntityChecker(endpoint, usr, pwd)
        # Hmmm - these look like they're needed for anat image set only,
        # so add  to have at object leve.
        self.anat_iri_gen = iri_generator(endpoint, usr, pwd, use_base36=use_base36)
        self.channel_iri_gen = iri_generator(endpoint, usr, pwd, use_base36=use_base36)
        self.channel_iri_gen.set_channel_config()
        self.commit_log = []

        #  Adding a dict of common classes and properties. (Should really just use KB lookup...)

        self.relation_lookup = {
            'depicts': 'http://xmlns.com/foaf/0.1/depicts',
            'in register with': 'http://purl.obolibrary.org/obo/RO_0002026',
            'is specified output of': 'http://purl.obolibrary.org/obo/OBI_0000312',
            'hasDbXref': 'http://www.geneontology.org/formats/oboInOwl#hasDbXref',
            'has_source': 'http://purl.org/dc/terms/source'
            }

        self.class_lookup = {
            'computer graphic': 'http://purl.obolibrary.org/obo/FBbi_00000224',
            'channel': 'http://purl.obolibrary.org/obo/fbbt/vfb/VFBext_0000014',
            'confocal microscopy': 'http://purl.obolibrary.org/obo/FBbi_00000251',
            'SB-SEM': 'http://purl.obolibrary.org/obo/FBbi_00000585',
            'TEM': 'http://purl.obolibrary.org/obo/FBbi_00000258',
            'X-ray computed tomography': 'http://purl.obolibrary.org/obo/FBbi_00001002'
            }

    def commit(self, ni_chunk_length=5000, ew_chunk_length=2000, verbose=False):
        """
        Commits nodes then edges. Populate commit_log, returns False if log has content,
        otherwise returns True.
        
        Parameters:
        ni_chunk_length (int): Chunk length for node commit.
        ew_chunk_length (int): Chunk length for edge commit.
        verbose (bool): Verbosity flag.
        
        Returns:
        bool: True if commit log is empty, False otherwise.
        """
        try:
            logging.debug("Starting commit process.")
            self.commit_log = []  # Reset commit log before starting

            logging.debug("Committing nodes with chunk length %d.", ni_chunk_length)
            self.ni.commit(verbose=verbose, chunk_length=ni_chunk_length)

            logging.debug("Committing edges with chunk length %d.", ew_chunk_length)
            self.ew.commit(verbose=verbose, chunk_length=ew_chunk_length)

            # Combine logs from ni and ew
            self.commit_log.extend(self.ni.log + self.ew.log)
            logging.debug("Combined commit log: %s", self.commit_log)

            if self.commit_log:
                logging.warning("Commit process encountered issues: %s", self.commit_log)
                return False
            else:
                logging.info("Commit process completed successfully with no issues.")
                return True

        except Exception as e:
            logging.error("An error occurred during the commit process: %s", e, exc_info=True)
            self.commit_log.append(str(e))
            return False

    def get_log(self):
        """
        Returns the commit log and clears it.

        Returns:
        list: The current commit log.
        """
        out = self.commit_log[:]
        self.commit_log = []
        return out

    @staticmethod
    def update_anat_id(anat_id, *args, **kwargs):
        if not isinstance(anat_id, str):
            return anat_id
    
        if anat_id.startswith("http"):
            # anat_id is an IRI
            iri = anat_id
            short_form = iri.split("/")[-1]  # Extract the part after the last "/"
        elif anat_id.startswith("VFB_"):
            # anat_id is a short form ID
            short_form = anat_id
            iri = f"http://virtualflybrain.org/reports/{short_form}"
        else:
            # anat_id is neither an IRI nor a short form ID
            return anat_id
        
        return {"iri": iri, "short_form": short_form}

    @staticmethod
    def update_channel_id(anat_id_dict):
        # Extract the IRI and short_form from the given dictionary
        iri = anat_id_dict['iri']
        short_form = anat_id_dict['short_form']
        
        # Replace 'VFB_' with 'VFBc_' in both the IRI and short_form
        new_iri = iri.replace('VFB_', 'VFBc_')
        new_short_form = short_form.replace('VFB_', 'VFBc_')
        
        # Create the updated dictionary
        updated_anat_id_dict = {
            'iri': new_iri,
            'short_form': new_short_form
        }
        
        return updated_anat_id_dict

    def add_anatomy_image_set(self,
                              dataset,
                              imaging_type,
                              label,
                              start,
                              template,
                              anat_id=None,
                              anatomical_type='',
                              anon_anatomical_types=None,
                              index=False,
                              center=(),
                              anatomy_attributes=None,
                              dbxrefs=None,
                              dbxref_strings=None,
                              image_filename='',
                              force_image_release=False,
                              match_on='short_form',
                              orcid='',
                              type_edge_annotations=None,
                              allow_duplicates=False,
                              hard_fail=True):
        """Adds typed inds for an anatomical individual and channel, 
        linked to each other and to the specified template.
        label: Name of anatomical individual
        imaging_type: One of: 'confocal microscopy', TEM, 'SB-SEM', 'computer graphic'.
           - SB-SEM = serial block face scanning EM (use for FIB-SEM data)
           - TEM = transmission electron microscopy (TEM) (use for CATMAID data)
           - 'computer graphic' is used for painted domains.
           If your image does not fit into these types, please post a ticket to request
           the list of supported types be extended.
        anatomical_type: classification of anatomical entity,
        anon_anatomical_types: list of r,o) tuples specifying anon
        anatomical types, where subject is the anatomical individual being created
        template: channel ID of the template to which the image is registered
        start: Start of range for generation of new accessions
        dbxrefs: dict of DB:accession pairs
        anatomy_attributes: Dict of property:value for anatomy node
        allow_duplicates: Boolean.  If True, allow overwiting of esiting images for flush and replace DataSets
        hard_fail: Boolean.  If True, throw exception for uknown entitise referenced in args"""
    
        logging.debug("Starting add_anatomy_image_set with parameters:")
        logging.debug(f"dataset={dataset}, imaging_type={imaging_type}, label={label}, start={start}, "
                      f"template={template}, anat_id={anat_id}, anatomical_type={anatomical_type}, "
                      f"anon_anatomical_types={anon_anatomical_types}, index={index}, center={center}, "
                      f"anatomy_attributes={anatomy_attributes}, dbxrefs={dbxrefs}, dbxref_strings={dbxref_strings}, "
                      f"image_filename={image_filename}, force_image_release={force_image_release}, "
                      f"match_on={match_on}, orcid={orcid}, type_edge_annotations={type_edge_annotations}, "
                      f"allow_duplicates={allow_duplicates}, hard_fail={hard_fail}")
    
        if anatomy_attributes is None: anatomy_attributes = {}
        if not force_image_release: anatomy_attributes['block'] = ["New Images"]
        if anon_anatomical_types is None: anon_anatomical_types = []
        if dbxrefs is None: dbxrefs = {}
        if dbxref_strings is None: dbxref_strings = []
    
        if type_edge_annotations is None: type_edge_annotations = {}
    
        if not template == 'self':
            self.ec.roll_entity_check(labels=['Individual'],
                                      match_on=match_on,
                                      query=template)
        self.ec.roll_entity_check(labels=['Class'],
                                  match_on=match_on,
                                  query=anatomical_type)
        self.ec.roll_entity_check(labels=['DataSet'],
                                  match_on=match_on,
                                  query=dataset)
        if dbxref_strings:
            # Add checking dbxref strings for ':'
            dbxrefs.update({x.split(':')[0]:x.split(':')[1] for x in dbxref_strings})
    
        for k in dbxrefs.keys():
            self.ec.roll_entity_check(labels=['Individual'],
                                      match_on=match_on,
                                      query=k)
    
        if orcid:
            self.ec.roll_entity_check(labels=['Person'],
                                      match_on=match_on,
                                      query=orcid)
    
        for ax in anon_anatomical_types:
            self.ec.roll_entity_check(labels=['Property'],
                                      match_on=match_on,
                                      query=ax[0])
            self.ec.roll_entity_check(labels=['Class'],
                                      match_on=match_on,
                                      query=ax[1])
    
        if dbxrefs:
            for db, acc in dbxrefs.items():
                self.ec.roll_dbxref_check(db, acc)
            if not self.ec.check(hard_fail=hard_fail):
                warnings.warn("Load fail: Cross-referenced entities already exist.")
                logging.debug("Load fail: Cross-referenced entities already exist.")
                return False
    
        if not self.ec.check(hard_fail=hard_fail):
            warnings.warn("Load fail: Unknown entities referenced.")
            logging.debug("Load fail: Unknown entities referenced.")
            return False
    
        if anat_id is None:
            anat_id = self.anat_iri_gen.generate(start)
            channel_id = self.channel_iri_gen.generate(start)
            logging.debug(f"Generated new anat_id: {anat_id}, channel_id: {channel_id}")
        else:
            anat_id = self.update_anat_id(anat_id)
            channel_id = self.update_channel_id(anat_id)
            logging.debug(f"Updated anat_id: {anat_id}, channel_id: {channel_id}")
            self.ec.roll_new_entity_check(labels=['Individual'],
                                          match_on=match_on,
                                          query=anat_id['short_form'], allow_duplicates=allow_duplicates)
            if not self.ec.check(hard_fail=hard_fail):
                warnings.warn("Load fail: Existing anat_id referenced.")
                logging.debug("Load fail: Existing anat_id referenced.")
                return False
        anat_id['label'] = label
        channel_id['label'] = label + '_c'
    
        anatomy_attributes['label'] = label
        self_labels = ["Individual"]
        if template == 'self':
            self_labels.append("Template")
    
        logging.debug(f"Adding node for anat_id: {anat_id}")
        self.ni.add_node(labels=self_labels,
                         IRI=anat_id['iri'],
                         attribute_dict=anatomy_attributes, allow_duplicates=allow_duplicates)
        
        logging.debug(f"Adding annotation axiom for anat_id: {anat_id[match_on]} to dataset: {dataset}")
        self.ew.add_annotation_axiom(s=anat_id[match_on],
                                     r='source',
                                     o=dataset,
                                     stype=':Individual',
                                     match_on=match_on,
                                     safe_label_edge=True)
    
        if dbxrefs:
            for db, acc in dbxrefs.items():
                logging.debug(f"Adding dbxref {db}:{acc} to anat_id: {anat_id['short_form']}")
                self.ew.add_annotation_axiom(s=anat_id['short_form'],
                                             r='hasDbXref',
                                             o=db,
                                             stype=':Individual',
                                             otype=':Individual',
                                             match_on='short_form',
                                             edge_annotations={'accession': [acc]},
                                             safe_label_edge=True)
        if orcid:
            logging.debug(f"Adding contributor orcid: {orcid} to anat_id: {anat_id['short_form']}")
            self.ew.add_annotation_axiom(s=anat_id['short_form'],
                                         r='contributor',
                                         o=orcid,
                                         stype=':Individual',
                                         match_on='short_form')
    
        logging.debug(f"Adding node for channel_id: {channel_id}")
        self.ni.add_node(labels=self_labels,
                         IRI=channel_id['iri'],
                         attribute_dict={'label': label + '_c'})
    
        logging.debug(f"Adding named type axiom for channel_id: {channel_id['short_form']}")
        self.ew.add_named_type_ax(s=channel_id['short_form'],
                                  o='VFBext_0000014',
                                  match_on='short_form')
    
        logging.debug(f"Adding anon type axiom for channel_id: {channel_id['iri']}")
        self.ew.add_anon_type_ax(s=channel_id['iri'],
                                 r=self.relation_lookup['is specified output of'],
                                 o=self.class_lookup[imaging_type])
    
        if anatomical_type:
            logging.debug(f"Adding named type axiom for anat_id: {anat_id[match_on]} to anatomical_type: {anatomical_type}")
            self.ew.add_named_type_ax(s=anat_id[match_on],
                                      o=anatomical_type,
                                      match_on=match_on,
                                      edge_annotations=type_edge_annotations)
    
        logging.debug(f"Adding fact depicts: {channel_id['iri']} to {anat_id['iri']}")
        self.ew.add_fact(s=channel_id['iri'],
                         r=self.relation_lookup['depicts'],
                         o=anat_id['iri'])
    
        edge_annotations = {}
        if index: edge_annotations['index'] = index
        if center: edge_annotations['center'] = center
        if image_filename: edge_annotations['filename'] = image_filename
    
        if template == 'self':
            template = channel_id['short_form']
    
        logging.debug(f"Adding fact in register with: {channel_id['short_form']} to template: {template}")
        self.ew.add_fact(s=channel_id['short_form'],
                         r=get_sf(self.relation_lookup['in register with']),
                         o=template,
                         edge_annotations=edge_annotations,
                         match_on='short_form',
                         safe_label_edge=True)
    
        for ax in anon_anatomical_types:
            logging.debug(f"Adding anon type axiom for anat_id: {anat_id[match_on]} with relation: {ax[0]} and object: {ax[1]}")
            self.ew.add_anon_type_ax(s=anat_id[match_on],
                                     r=ax[0],
                                     o=ax[1],
                                     match_on='short_form')
    
        logging.debug("Anatomy image set added successfully.")
        return {'channel': channel_id, 'anatomy': anat_id }

    def add_dataSet(self, name,
                    license,
                    short_form,
                    pub='',
                    description='',
                    dataset_spec_text='',
                    site='',
                    schema='image',
                    match_on='short_form'):

        """Add a new dataset to the DB:
        required ARGS:
            nme = Descriptive name for dataset
            short_form = readable short_form for dataset
            license = short_form for license
        optional KWARGS
            pub = (optional) short_form (FBrf) for pub describing dataset.
            description = Some text describing the dagtaset
            dataset_spec_text = Some text to be added to the description of individuals in the dataset
            site = short_form identifier for site.
        """

        self.ec.roll_entity_check(labels=['Individual'],
                                  match_on=match_on,
                                  query=license)
        if pub:
            self.ec.roll_entity_check(labels=['Individual'],
                                      match_on=match_on,
                                      query=pub)
        if site:
            self.ec.roll_entity_check(labels=['Individual'],
                                      match_on=match_on,
                                      query=site)

        if not self.ec.check():
            return False

        dataset_id = {'iri': map_iri('data') + short_form , 'short_form': short_form }
        self.ni.add_node(labels=['Individual', 'DataSet'],
                         IRI=dataset_id['iri'],
                         attribute_dict={
                             'label': name,
                             'short_form': short_form,
                             'description': [description],
                             'dataset_spec_text': [dataset_spec_text],
                             'schema': schema })
#       self.ni.commit()
        self.ew.add_annotation_axiom(s=short_form,
                                     r='license',
                                     o=license,
                                     stype=':Individual',
                                     otype=':Individual:License',
                                     match_on=match_on,
                                     safe_label_edge=True)
        if site:
            self.ew.add_annotation_axiom(s=short_form,
                                         r='hasDbXref',
                                         o=site,
                                         stype=':Individual',
                                         otype=':Individual',
                                         match_on='short_form',
                                         safe_label_edge=True)
        if pub:
            self.ew.add_annotation_axiom(s=short_form,
                                         r='references',
                                         o=pub,
                                         stype=':Individual',
                                         otype=':Individual:pub',
                                         match_on='short_form',
                                         safe_label_edge=True)

        return dataset_id






# Specs for a fb_feature_update
## Pull current feature nodes from DB
#   query = "SELECT uniquename, name, is_obsolete from feature"

#class fb_feature_update(kb_writer):   
    

# def add_ind(self, iri, short_form, label, synonyms = [], additional_attributes = {}):
#     out = "MERGE (i:Individual { IRI: '%s'} ) " \
#             "SET i.short_form = '%s' " \
#             "SET i.label = '%s' " % (iri, short_form, label)
#     if synonyms:
#             out += "SET i.synonyms = %s  " % str(synonyms)     
#     out += self._set_attributes_from_dict('i', additional_attributes)
#     return out

# def add_relation_node(self, iri, short_form, label):
#     return "MERGE (i:Relation { IRI: '%s'} ) " \
#             "SET i.short_form = '%s' " \
#             "SET i.label = '%s' " % (iri, short_form, label)
