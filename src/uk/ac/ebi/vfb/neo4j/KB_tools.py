'''
Created on Mar 6, 2017

@author: davidos
'''
import warnings
import re
import json
#import psycopg2
import requests
from .neo4j_tools import neo4j_connect, results_2_dict_list
from .SQL_tools import get_fb_conn, dict_cursor
from ..curie_tools import map_iri


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

def gen_id(idp, ID, length, id_name):
    """
    Generates an ID of form <idp>_<padded_accession>
    ARG1: idp (string), 
    ARG 2 starting ID number (int), 
    ARG3, length of numeric portion ID, 
    ARG4 an id:name hash"""
    def gen_key(ID, length):  # This function is limited to the scope of the gen_id function.
        dl = len(str(ID)) # coerce int to string.
        k = idp+'_'+(length - dl)*'0'+str(ID)
        return k
    
    k = gen_key (ID, length)
    while k in id_name:
        ID += 1
        k = gen_key(ID, length)
    return {'short_form' : k, 'acc_int' : ID} # useful to return ID to use for next round.


class kb_writer (object):
      
    def __init__(self, endpoint, usr, pwd, hard_fail=False):
        self.nc = neo4j_connect(endpoint, usr, pwd)
        self.statements = []
        self.output = []

    def _commit(self, verbose=False, chunk_length=5000):
        """Commits Cypher statements stored in object.
        Flushes existing statement list.
        Returns REST API output.
        Optionally set verbosity and chunk length for commits."""
        self.output = self.nc.commit_list_in_chunks(
                                      statements=self.statements,
                                      verbose=verbose,
                                      chunk_length=chunk_length)
        self.statements = []
        return self.output

    def commit(self, verbose=False, chunk_length=5000):
        return self._commit(verbose, chunk_length)
    

    def escape_string(self, strng):
        if type(strng) == str:
            strng = re.sub("'", "\\'", strng)
            strng = re.sub(r'\\', r'\\\\', strng)
        return strng
  
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
    # Making this 
        
    def configure(self, idp, acc_length, base):
        self.acc_length = acc_length
        self.idp = idp
        self.id_name = {}
        self.base = base
        # Should I really be assuming everything has a short_form?
        self.statements.append("MATCH (i:Individual) WHERE i.short_form =~ '%s_[0-9]{%d}' " \
                               "RETURN i.short_form as short_form, i.label as label" % (idp, acc_length)) # Note POSIX regex rqd       
        r = self.commit()
        if r:
            results = results_2_dict_list(r)
            for res in results:
                self.id_name[res['short_form']] = res['label']
            return True
        else:
            warnings.warn("No existing ids match the pattern %s_%s" % (idp, 'n'*acc_length))
            return False

    def set_default_config(self):
        self.configure(idp = 'VFB', acc_length = 8, base = map_iri('vfb'))

    def set_channel_config(self):
        self.configure(idp='VFBc', acc_length = 8, base = map_iri('vfb'))

    def generate(self, start, label=''):
        ID = gen_id(idp = self.idp, ID = start, length = self.acc_length, id_name = self.id_name)
        short_form = ID['short_form']
        iri =  self.base + short_form
        self.id_name[short_form] = label
        return { 'iri': iri, 'short_form': short_form }
    

class kb_owl_edge_writer(kb_writer):
    """A class wrapping methods for updating imported entities in the KB.
    Constructor: kb_owl_edge_writer(endpoint, usr, pwd)
    """

    def __init__(self, endpoint, usr, pwd, hard_fail = False):
        self.nc = neo4j_connect(endpoint, usr, pwd)
        self.statements = []
        self.output = []
        # An objecty representation of properties might be more easily maintainable.
        self.properties = {} # Dict of properties.
        self.triples = {} # Dict of lists of args to a triples method, keyed on op.
        self.hard_fail = hard_fail

    def check_properties(self):
        """Check whether properties used in triples have a corresponding Property node.
        Lookup is via property specified in match_on arg in an add_triple method.
        Purge triples using properties not found in the DB from the stack."""

        statements = []
        for k,v in self.properties.items():
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
                    warnings.warn('%s in KB but has no iri!' % d['key'])
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
            warnings.warn("Unknown property %s: Can't add triple %s, %s, %s." % (prop,
                                                                              t['o'],
                                                                              prop,
                                                                              t['s']))

    def _add_triple(self, s, r, o, rtype, stype, otype,
                    edge_annotations=None, match_on="iri", safe_label_edge=False):
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
                rel = re.sub(' ', '_', rel_map['label'])
            else:
                    rel = rel_map[t['match_on']]

            out = "OPTIONAL MATCH (s%s { %s:'%s' }) " % (t['stype'],t['match_on'], t['s'])
            out += "OPTIONAL MATCH (o%s { %s:'%s' }) " % (t['otype'], t['match_on'], t['o'])
            out += "FOREACH (a IN CASE WHEN s IS NOT NULL THEN [s] ELSE [] END | " \
                   "FOREACH (b IN CASE WHEN o IS NOT NULL THEN [o] ELSE [] END | "
            if t['safe_label_edge']:
                out += "MERGE (a)-[re:%s]->(b) SET re.type = '%s'" % (rel, t['rtype'])  # Might need work?
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
                          edge_annotations=None, match_on="iri", safe_label_edge=False):
        if edge_annotations is None:
            edge_annotations = {}
        rtype = 'Related'
        self._add_triple(s, r, o, rtype, stype, otype,
                         edge_annotations, match_on, safe_label_edge=safe_label_edge)

    def add_annotation_axiom(self, s, r, o, edge_annotations=None, match_on="iri", safe_label_edge=False):
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
        stype = ''
        otype = '' # This should really be an individual, but some changes to DB are needed first.
        self._add_triple(s, r, o, rtype, stype, otype,
                         edge_annotations, match_on, safe_label_edge=safe_label_edge)

    def add_fact(self, s, r, o, edge_annotations = None,
                 match_on="iri", safe_label_edge=False):

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
                
    def add_anon_type_ax(self, s, r, o, edge_annotations=None,
                         match_on="iri", safe_label_edge=False):
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
                               match_on="iri", safe_label_edge=False):
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
        self.properties = {} # Dict of properties
        self.triples = {} # Dict of lists of triples keyed on property
        return out

    def test_edge_addition(self):
        """Tests lists of return values from RESTFUL API for edge creation
         by checking
        """
        dc = results_2_dict_list(self.output)
        missed_edges = [x['match_count'] for x in dc if x and (0 in x['match_count'].values())]
        if missed_edges:
            for e in missed_edges:
                warnings.warn("No match found for %s in %s" % (str([k for k,v in e.items() if not v]),
                                                               str(e)))
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
            
    def add_node(self, labels, IRI, attribute_dict=None):
        """Adds or updates a node.
        Node uniqueness specified by IRI + labels.
        Derives short_form using has or / as delimiter
        Adds/Updates attributes to those specified in the attribute dict
        """
        if attribute_dict is None: attribute_dict = {}
        short_form = re.split('[#/]', IRI)[-1]
        statement = "MERGE (n:%s { iri: '%s' }) set n.short_form = '%s'" % ((':'.join(labels)),
                                                     IRI, short_form)
        statement += self._set_attributes_from_dict(var = 'n', 
                                                    attribute_dict = attribute_dict)
        self.statements.append(statement)

    
    def update_from_obograph(self, file_path = '', url = ''):
        """Update property and class nodes from an OBOgraph file
        (currently does not distinguish OPs from APs!)
        Only updates from pimary graph (i.e. ignores imports)
        """
        ## Get JSON, assuming only primary graph should be used for updating
        ## ie: imports ignored.
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
                warnings.warn("URL connection issue %s %s" % (r.status_code, 
                                                              r.reason))
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
                elif node['type'] == 'PROPERTY':
                    labels = ['Property']
                else:
                    continue
            # Split URL -> base & short_form
            m = re.findall('.+(#|/)(.+?)$', node['id'])
            attribute_dict['short_form'] =  m[0][1]
            if 'lbl' in node.keys(): attribute_dict['label']=  node['lbl']
            if 'meta' in node.keys():
                if 'deprecated' in node['meta'].keys():
                    attribute_dict['is_obsolete'] = node['meta']['deprecated']
            ## Update nodes.
            self.add_node(labels, IRI, attribute_dict)
        self.check_for_obsolete_nodes_in_use()
        return True

    def check_for_obsolete_nodes_in_use(self):
        m = "MATCH (c:Class)-[r]-(fu) WHERE c.is_obsolete=True " \
            "RETURN c.label, c.IRI"
        q = results_2_dict_list(self.nc.commit_list([m]))
        if q:
            for r in q:
                warnings.warn("%s, %s is obsolete but in use." % 
                              (r['c.label'], r['c.IRI']))
            return False
        else:
            print("No obsolete nodes in use.")
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

    """Check for the existance of nodes"""

    def roll_check(self, labels, query, match_on='short_form'):
        """Roll a check and add it to the stack.
        labels = list of Neo4J labels match on
        match_on = property to match_on (default = short_form)
        query = Value of property matched on for target entity.
        """
        lstring = ':'.join(labels)
        self.statements.append("OPTIONAL MATCH (n:%s { %s : '%s'}) return n.short_form as result, '%s' as query" % (lstring, match_on, query, query))

    def check_entities(self, hard_fail=False):
        """Run checks in the stack then empty the stack.
        If hard_fail = True, raise exception if any check in the stack fails."""
        dc = results_2_dict_list(self.commit())
        out = {}
        for d in dc:
            if d['result']:
                out[d['query']]=True
            else:
                out[d['query']]=False
                warnings.warn("Unknown entity %s" %d['query'])
        if False in out.values():
            if hard_fail:

                raise Exception('Uknown entities.')
            else:
                return False
        else:
            return True
    
class KB_pattern_writer(object):
    """A wrapper class for adding subgraphs following some pre-specified
    schema pattern.
    """
    
    def __init__(self, endpoint, usr, pwd):
        self.ew = kb_owl_edge_writer(endpoint, usr, pwd)
        self.ni = node_importer(endpoint, usr, pwd)
        self.iri_gen = iri_generator(endpoint, usr, pwd)
        self.ec = EntityChecker(endpoint, usr, pwd)
        # Hmmm - these look like they're needed for anat image set only.
        self.anat_iri_gen = iri_generator(endpoint, usr, pwd)
        self.anat_iri_gen.set_default_config()
        self.channel_iri_gen = iri_generator(endpoint, usr, pwd)
        self.channel_iri_gen.configure(idp='VFBc',
                                       acc_length=8,
                                       base=map_iri('vfb'))

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
            'SB-SEM': 'http://purl.obolibrary.org/obo/FBbi_00000585'
            }

    def commit(self, ni_chunk_length=5000, ew_chunk_length=2000, verbose=False):

        self.ni.commit(verbose=verbose, chunk_length=ni_chunk_length)
        self.ew.commit(verbose=verbose, chunk_length=ew_chunk_length)

    def add_anatomy_image_set(self,
                              dataset,
                              imaging_type,
                              label,
                              start,
                              template,
                              anatomical_type='',
                              index=False,
                              center=(),
                              anatomy_attributes=None,
                              dbxrefs=None,
                              image_filename='',
                              match_on='short_form',
                              orcid = '',
                              hard_fail=False):
        """Adds typed inds for an anatomical individual and channel, 
        linked to each other and to the specified template.
        label: Name of anatomical individual
        imaging_type: One of: 'confocal microscopy', 'SB-SEM', 'computer graphic'.
           - SB-SEM = serial block face scanning EM, this is used for CATMAID data
           - 'computer graphic' is used for painted domains.
           If your image does not fit into these types, please post a ticket to request
           the list of supported types be extended.
        template: channel ID of the template to which the image is registered
        start: Start of range for generation of new accessions
        dbxrefs: dict of DB:accession pairs
        anatomy_attribute = {}
        hard_fail: Boolean.  If True, throw exception for uknown entitise referenced in args"""

        if anatomy_attributes is None: anatomy_attributes = {}
        if dbxrefs is None: dbxrefs = {}

        self.ec.roll_check(labels=['Individual'],
                           match_on=match_on,
                           query=template)
        self.ec.roll_check(labels=['Class'],
                           match_on=match_on,
                           query=anatomical_type)
        self.ec.roll_check(labels=['DataSet'],
                           match_on=match_on,
                           query=dataset)
        for k in dbxrefs.keys():
            self.ec.roll_check(labels=['Site'],
                               match_on=match_on,
                               query=k)

        if orcid:
            self.ec.roll_check(labels=['Person'],
                               match_on=match_on,
                               query=orcid)

        if not self.ec.check_entities(hard_fail=hard_fail):
            warnings.warn("Unknown entities referenced in, not adding.")
            return False

        anat_id = self.anat_iri_gen.generate(start)

        anat_id['label'] = label
        channel_id = self.channel_iri_gen.generate(start)
        channel_id['label'] = label + '_c'

        anatomy_attributes['label'] = label


        self.ni.add_node(labels=['Individual'],
                         IRI=anat_id['iri'],
                         attribute_dict=anatomy_attributes)
        #dataset_short_form = self.ni.nc.commit_list(["MATCH (ds:DataSet) WHERE ds.label = %s RETURN ds.short_form" % dataset])
        self.ew.add_annotation_axiom(s=anat_id[match_on],
                                     r='source',
                                     o=dataset,
                                     match_on=match_on,
                                     safe_label_edge=True)

        if dbxrefs:
            for db, acc in dbxrefs.items():
                self.ew.add_annotation_axiom(s=anat_id['short_form'],
                                             r='hasDbXref',
                                             o=db,
                                             match_on='short_form',
                                             edge_annotations={'accession': acc},
                                             safe_label_edge=True
                                             )
        if orcid:
            self.ew.add_annotation_axiom(s=anat_id['short_form'],
                                         r='contributor',
                                         o=orcid,
                                         match_on='short_form')  # This assumes matching on short form!

        self.ni.add_node(labels=['Individual'],
                         IRI=channel_id['iri'],
                         attribute_dict={'label': label + '_c'}
                         )
        # Add a query to look up template channel, assuming template anat ind spec
        #q = "MATCH (c:Individual)-[:Related { short_form : 'depicts' }]" \
        #    "->(t:Individual { iri : '%s' }) RETURN c.iri" % template
        #x = results_2_dict_list(self.ni.nc.commit_list([q]))
        #template = x['c.iri']

        # Add typing as channel.  This takes no vars so match_on can be fixed.
        self.ew.add_named_type_ax(s=channel_id['short_form'],
                                  o='VFBext_0000014',
                                  match_on='short_form')


        # Imaging modality - currently works on internal lookup in script.  Should probably be dynamic with DB
        self.ew.add_anon_type_ax(s=channel_id['iri'],
                                 r=self.relation_lookup['is specified output of'],
                                 o=self.class_lookup[imaging_type])

        if anatomical_type:
            self.ew.add_named_type_ax(s=anat_id[match_on],
                                      o=anatomical_type,
                                      match_on=match_on)
        # Add facts

        # This takes no vars so match_on can be fixed.
        self.ew.add_fact(s=channel_id['iri'],
                         r=self.relation_lookup['depicts'],
                         o=anat_id['iri'])

        edge_annotations = {}
        if index: edge_annotations['index'] = index
        if center: edge_annotations['center'] = center
        if image_filename: edge_annotations['filename'] = image_filename

        # if template == 'self':
        #    template = channel_iri
        self.ew.add_fact(s=channel_id['short_form'],
                         r=get_sf(self.relation_lookup['in register with']),
                         o=template,
                         edge_annotations=edge_annotations,
                         match_on='short_form',
                         safe_label_edge=True)
        return {'channel': channel_id, 'anatomy': anat_id }

    def add_dataSet(self, name, license, short_form, pub='',
                    description='', dataset_spec_text='', site=''):

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

        self.ni.add_node(labels=['Individual', 'DataSet'],
                         IRI=map_iri('data') + short_form,
                         attribute_dict={
                             'label': name,
                             'short_form': short_form,
                             'description': description,
                             'dataset_spec_text': dataset_spec_text})
        self.ni.commit()
        self.ew.add_annotation_axiom(s=name,
                                     r='license',
                                     o=license,
                                     match_on='short_form',
                                     safe_label_edge=True)
        if site:
            self.ew.add_annotation_axiom(s=name,
                                         r='hasDbXref',
                                         o=site,
                                         match_on='short_form',
                                         safe_label_edge=True)
        if pub:
            self.ew.add_annotation_axiom(s=name,
                                         r='references',
                                         o=pub,
                                         match_on='short_form',
                                         safe_label_edge=True)





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
