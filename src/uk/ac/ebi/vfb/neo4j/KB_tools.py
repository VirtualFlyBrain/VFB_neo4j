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
#    * (:Individual)-[:Related { IRI: '', label: ''}]-(:Individual)  -> OWL FACT (OPA)
#    * (:Individual)-[:Related { IRI: '', label: ''}]-(:Class) -> OWL Type: R some C
#    * (:Class)-[:Related { IRI: '', label: ''}]-(:Individual) -> OWL SubClassOf: R value I

# Match statements checks for all relevant entites, including relations if applicable. Implementing methods should 
# check return values and warn/fail as appropriate if no match.

# TODO: Add lookup for attributes -> Properties.  Ideally this would be with a specific cypher label for APs.
# May want to follow a prefixed pattern to indicate OWL compatible APs.


    

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

class iri_generator():
    
    def __init__(self, endpoint, usr, pwd):
        self.nc = neo4j_connect(endpoint, usr, pwd)
        
    def configure(self, idp, acc_length, base):
        self.acc_length = acc_length
        self.idp = idp
        self.id_name = {}
        self.base = base
        self.statements.append("MATCH (i:Individual) WHERE i.short_form =~ '%s_[0-9]{%d}' " \
                               "RETURN i.short_form as short_form, i.label as label" % (idp, acc_length)) # Note POSIX regex rqd       
        r = self.commit()
        if r:
            results = results_2_dict_list(r)
            for res in results:
                self.id_name[res['short_form']] = res['label']
            return True
        else:
            warnings.warn("No ids match the pattern %s_%s" % (idp, 'n'*acc_length))
            return False
            

    def set_default_config(self):
        self.configure(idp = 'VFB', acc_length = 8, base = map_iri('vfb'))
        
    def generate(self, start):
        ID = gen_id(idp = self.idp, ID = start, length = self.acc_length, id_name = self.id_name)
        short_form = ID['short_form']
        iri =  self.base + short_form
        return { 'iri': iri, 'short_form': short_form}
    
    

class kb_writer (object):
      
    def __init__(self, endpoint, usr, pwd):
        self.nc = neo4j_connect(endpoint, usr, pwd)
        self.statements = []
        self.output = []
        self.properties = set([])
        
    def _commit(self, verbose = False, chunk_length = 5000):
        """Commits Cypher statements stored in object.
        Flushes existing statement list.
        Returns REST API output.
        Optionally set verbosity and chunk length for commits."""
        self.output = self.nc.commit_list_in_chunks(
                                      statements  = self.statements, 
                                      verbose = verbose, 
                                      chunk_length = chunk_length)
        self.statements = []
        return self.output

    def commit(self, verbose = False, chunk_length = 5000):
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


class kb_owl_edge_writer(kb_writer):
    """A class wrapping methods for updating imported entities in the KB.
    Constructor: kb_owl_edge_writer(endpoint, usr, pwd)
    """
    
    def check_proprties(self):
        ## Not well thought out.  Consider removing.
        """OWL edge IRIs must correspond to IRIs of property nodes (loaded from source ontologies).
        self.properties = list of properties being added during edge addition."""
        q = self.nc.commit_list(["MATCH (n) WHERE n.iri in %s RETURN n.iri" % (str(list(self.properties)))])
        if q:
            in_kb = [x['row'][0] for x in q[0]['data'] if q[0]['data']] # YUK!
            not_in_kb = self.properties.difference(set(in_kb))
            if not_in_kb:
                warnings.warn("Not in KB! %s" % str(not_in_kb))
                return False
            else:
                return True
        else: 
            return False
            
    def _add_related_edge(self, s, r, o, stype, otype, edge_annotations = {}, match_on = "iri"):
        # running edge check for each edge addn - safe by slooow.
        if match_on not in ['iri', 'label', 'short_form']:
            raise Exception("Illegal match property '%s'. " \
                            "Allowed match properties are 'iri', 'label', 'short_form'" % match_on)
        out =  "MATCH (s:{stype} {{ {match_on}:'{s}' }} ), (rn:Property {{ {match_on}: '{r}' }}), " \
          "(o:{otype} {{ {match_on}:'{o}' }} ) ".format(**locals())
        out += "MERGE (s)-[re:Related { %s: '%s'}]-(o) " % (match_on, r)
        out += self._set_attributes_from_dict('re', edge_annotations)        
        out += "SET re.label = rn.label SET re.short_form = rn.short_form "
        out += "RETURN '%s', '%s', '%s' " % (s,r,o) # returning input for ref in debugging
        # If the match fails, no rows are returned, but s,r,o are column headers, 
        # so can be used for warnings/exceptions
        
        self.statements.append(out)
    
    def add_fact(self, s, r, o, edge_annotations = {}, match_on = "iri"):

        """Add OWL fact to statement queue.
        s=subject individual iri, 
        r= relation (ObjectProperty) iri,
        o = object individual iri.
        Optionally add edge annotations specified as key value 
        pairs in dict."""
        self._add_related_edge(s, r, o, stype = "Individual", otype = "Individual", 
                               edge_annotations = edge_annotations, 
                               match_on = match_on)
                
    def add_anon_type_ax(self, s, r, o, edge_annotations = {}, match_on = "iri"):
        """Add anonymous OWL Type statement queue.
        s= subject individual iri, 
        r= relation (ObjectProperty) iri,
        o = object Class iri.
        Optionally add edge annotations specified as key value 
        pairs in dict."""
        self._add_related_edge(s, r, o, stype = "Individual", otype = "Class", 
                               edge_annotations = edge_annotations, 
                               match_on = match_on)
    
        
    def add_named_type_ax(self, s,o, match_on = "iri"):
        self.statements.append(
                               "MATCH (s:Individual {{ {match_on}: '{s}' }} ), (o:Class {{ {match_on}: '{o}' }} ) " \
                               "MERGE (s)-[:INSTANCEOF]-(o) " \
                               "RETURN '{s}', '{o}'".format(**locals()))
                
    def add_anon_subClassOf_ax(self, s,r,o, edge_annotations = {}, match_on = "iri"):
        ### Should probably only support adding individual:individual edges in KB...
        self._add_related_edge(s, r, o, stype = "Class", otype = "Class", 
                               edge_annotations = edge_annotations, 
                               match_on = match_on)

    def add_named_subClassOf_ax(self, s,o):
        return "MATCH (s:Class { iri: '%s'} ), (o:Class { iri: '%s'} ) " \
                "MERGE (s)-[:SUBCLASSOF]-(o)" % (s, o)            
    
    
    def commit(self, verbose=False, chunk_length=5000):
        """Commit and test edge addition"""
        self._commit(verbose, chunk_length)
        self.test_edge_addition()
        
        
    def test_edge_addition(self):
        """Tests lists of return values from RESTFUL API for edge creation
         by checking "relationships_created": as a boolean, generates warning
        """
       
        missed_edges = [x['columns'] for x in self.output if not x['data']]
        if missed_edges:
            for e in missed_edges:
                warnings.warn("Edge not added. Something doesn't match here: %s" % str(e))
            return False
        else:
            return True

class node_importer(kb_writer):
    """A class wrapping methods for updating imported entities in the KB,
    e.g. from ontologies, FlyBase, CATMAID.
    Constructor: owl_import_updater(endpoint, usr, pwd)
    """
        
    def add_constraints(self, uniqs= {}, indexes = {} ):
        """Specify addition uniqs and indexes via dicts.
        { label : [attributes] } """
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
            
    def add_node(self, labels, IRI, attribute_dict = {}):
        """Adds or updates a node.
        Node uniqueness specified by IRI + labels.
        Adds/Updates attributes to those specified in the attribute dict
        """
        statement = "MERGE (n:%s { iri: '%s' }) " % ((':'.join(labels)), 
                                                     IRI)
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
            primary_graph = obographs['graphs'][0]
        elif url:
            r = requests.get(url)
            if r.status_code == 200:
                obographs = r.json()
                primary_graph = obographs['graphs'][0] # Add a check for success here!
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
    
class KB_pattern_writer(object):
    """A wrapper class for adding subgraphs following some pre-specified
    schema pattern.
    """
    
    def __init__(self, endpoint, usr, pwd):
        self.ew = kb_owl_edge_writer(endpoint, usr, pwd)
        self.ni = node_importer(endpoint, usr, pwd)    
        self.anat_iri_gen = iri_generator(endpoint, usr, pwd)
        self.anat_iri_gen.set_default_iri_gen_config()
        self.channel_iri_gen = iri_generator(endpoint, usr, pwd)
        self.channel_iri_gen.configure_iri_gen(idp = 'VFBc', 
                                                acc_length = 8, 
                                                base = map_iri('vfb'))
       
    def add_anatomy_image_set(self, 
                                  image_type, 
                                  label, 
                                  start,
                                  template,
                                  anatomical_type = '',
                                  attributes = {}):
        """Adds typed inds for an anatomical individual and channel, 
        linked to each other and to the specified template.  
        The label argument is used to set the label for the anatomical 
        individual and to generate a derived label for the channel. Start
        specifies the starting range for adding new accessions. 
        Optionally specify additional attributes for the anatomical 
        individual as a dict."""
        
        # TBD: Should this really all run on IRIs?
        
        ### IRI gen is an issue because node_importer assumes a single
        #### ID scheme, but we need different schemes for anatomy and 
        #### channel
        anat_iri = self.anat_iri_gen.generate(start)['iri']
        channel_iri = self.channel_iri_gen.generate(start)['iri']        
        attributes['label'] = label
        self.ni.add_node(labels = ['Individual'], 
                          IRI = anat_iri,
                          attribute_dict = attributes)
        self.ni.add_node(labels = ['Individual'], 
                          IRI = channel_iri,
                          attribute_dict= { 'label' : label + '_c' } )
        self.ni.commit()
        # Add a query to look up template channel, assuming template anat ind spec
        q = "MATCH (c:Individual)-[:Related { short_form = 'depicts' }]" \
            "->(t:Individual { iri : '%s' ) RETURN c.iri" % template
        x = results_2_dict_list(self.ni.nc.commit_list([q]))
        template = x['c.iri']
        
        # Some relation IRIs:
        
        image_typing_relation = ''  # oddity of schema.
        depicts = ''
        in_reg_with = ''
        
        self.ew.add_anon_type_ax(s = channel_iri, 
                            r = image_typing_relation,
                            o = anatomical_type)
        if anatomical_type:
            self.sew.add_named_type_ax(s = anat_iri, o = anatomical_type)
        # Add facts    
        self.ew.add_fact(s = channel_iri, r = depicts , o = anat_iri)
        self.ew.add_fact(s = channel_iri , r = in_reg_with, o = template) 



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
