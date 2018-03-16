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
      
    def __init__(self, endpoint, usr, pwd):
        self.nc = neo4j_connect(endpoint, usr, pwd)
        self.statements = []
        self.output = []
        self.properties = set([])
        
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

        
    def generate(self, start, label = ''):
        ID = gen_id(idp = self.idp, ID = start, length = self.acc_length, id_name = self.id_name)
        short_form = ID['short_form']
        iri =  self.base + short_form
        self.id_name[short_form] = label
        return { 'iri': iri, 'short_form': short_form }
    

class kb_owl_edge_writer(kb_writer):
    """A class wrapping methods for updating imported entities in the KB.
    Constructor: kb_owl_edge_writer(endpoint, usr, pwd)
    """

    #TODO - refactor for speed:

    # On commit (i.e. in batch)
    # Run batch lookup for properties first => additional data + check for existence
    # MATCH only on S & O.
    # Merge pulls from property data model.
    
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

    def _add_triple(self, s, r, o, rtype, stype, otype, edge_annotations = {}, match_on = "iri"):
        if match_on not in ['iri', 'label', 'short_form']:
            raise Exception("Illegal match property '%s'. " \
                            "Allowed match properties are 'iri', 'label', 'short_form'" % match_on)

        out = "OPTIONAL MATCH (s{stype} {{ {match_on}:'{s}' }} ) " \
              "OPTIONAL MATCH (rn:Property {{ {match_on}: '{r}' }}) " \
              "OPTIONAL MATCH (o{otype} {{ {match_on}:'{o}' }} ) ".format(**locals())
        out += "FOREACH (a IN CASE WHEN s IS NOT NULL THEN [s] ELSE [] END | " \
               "FOREACH (b IN CASE WHEN o IS NOT NULL THEN [o] ELSE [] END | " \
               "FOREACH (c IN CASE WHEN rn IS NOT NULL THEN [rn] ELSE [] END | "
        out += "MERGE (a)-[re%s { %s: c.%s }]->(b) " % (rtype, match_on, match_on)  # Might need work?
        out += self._set_attributes_from_dict('re', edge_annotations)
        if not match_on == 'label':
            out += "SET re.label = c.label "
        if not match_on == 'short_form':
            out += "SET re.short_form = c.short_form "
        if not match_on == 'iri':
            out += "SET re.iri = c.iri"
        out += "))) RETURN { `%s`: count(s), `%s`: count(rn), `%s`: count(o) } as match_count" % (s, r, o)
        self.statements.append(out)

    def _add_related_edge(self, s, r, o, stype, otype, edge_annotations={}, match_on="iri"):
        # running edge check for each edge addn - safe by slooow.
        rtype = ':Related'
        self._add_triple(s, r, o, rtype, stype, otype, edge_annotations, match_on)

    def add_annotation_axiom(self, s, r, o, edge_annotations = {}, match_on = "iri"):
        """Used to link an OWL entity to an Individual via an annotation axiom."""
        rtype = ':Annotation'
        stype = ''
        otype = '' # This should really be an individual, but some changes to DB are needed first.
        self._add_triple(s, r, o, rtype, stype, otype, edge_annotations, match_on)

    def add_fact(self, s, r, o, edge_annotations = {}, match_on = "iri"):

        """Add OWL fact to statement queue.
        s=subject individual iri, 
        r= relation (ObjectProperty) iri,
        o = object individual iri.
        Optionally add edge annotations specified as key value 
        pairs in dict."""
        self._add_related_edge(s, r, o, stype = ":Individual", otype = ":Individual",
                               edge_annotations = edge_annotations, 
                               match_on = match_on)
                
    def add_anon_type_ax(self, s, r, o, edge_annotations = {}, match_on = "iri"):
        """Add anonymous OWL Type statement queue.
        s= subject individual iri, 
        r= relation (ObjectProperty) iri,
        o = object Class iri.
        Optionally add edge annotations specified as key value 
        pairs in dict."""
        self._add_related_edge(s, r, o, stype = ":Individual", otype = ":Class",
                               edge_annotations = edge_annotations, 
                               match_on = match_on)
    
        
    def add_named_type_ax(self, s,o, match_on = "iri"):
        out = "OPTIONAL MATCH (s:Individual {{ {match_on}:'{s}' }} ) " \
              "OPTIONAL MATCH (o:Class {{ {match_on}:'{o}' }} ) ".format(**locals())
        out += "FOREACH (a IN CASE WHEN s IS NOT NULL THEN [s] ELSE [] END | " \
               "FOREACH (b IN CASE WHEN o IS NOT NULL THEN [o] ELSE [] END | " \
               "MERGE (a)-[:INSTANCEOF]->(b) "
        out += ")) RETURN { `%s`: count(s), `%s`: count(o) } as match_count" % (s, o)
        self.statements.append(out)

    def add_anon_subClassOf_ax(self, s,r,o, edge_annotations = {}, match_on = "iri"):
        ### Should probably only support adding individual:individual edges in KB...
        self._add_related_edge(s, r, o, stype = ":Class", otype = ":Class",
                               edge_annotations = edge_annotations, 
                               match_on = match_on)

    def add_named_subClassOf_ax(self, s,o, match_on = "iri"):
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
        #TODO - update to latest
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
        Derives short_form using has or / as delimiter
        Adds/Updates attributes to those specified in the attribute dict
        """
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
    
class KB_pattern_writer(object):
    """A wrapper class for adding subgraphs following some pre-specified
    schema pattern.
    """
    
    def __init__(self, endpoint, usr, pwd):
        self.ew = kb_owl_edge_writer(endpoint, usr, pwd)
        self.ni = node_importer(endpoint, usr, pwd)    
        self.anat_iri_gen = iri_generator(endpoint, usr, pwd)
        self.anat_iri_gen.set_default_config()
        self.channel_iri_gen = iri_generator(endpoint, usr, pwd)
        self.channel_iri_gen.configure(idp='VFBc',
                                       acc_length=8,
                                       base=map_iri('vfb'))

        #  Adding a dict of common classes and properties

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
            'confocal microscopy' : 'http://purl.obolibrary.org/obo/FBbi_00000251',
            'SB-SEM' : 'http://purl.obolibrary.org/obo/FBbi_00000585'
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
                              anatomy_attributes={},
                              dbxrefs={},
                              image_filename = '',
                              match_on = 'short_form'
                              ):
        """Adds typed inds for an anatomical individual and channel, 
        linked to each other and to the specified template.
        label: Name of anatomical individual
        imaging_type: a relevant FBbi term e.g. 'confocal microscopy', 'electron microscopy'
        template: channel ID of the template to which the image is registered
        start: Start of range for generation of new accessions
        dbxrefs: dict of DB:accession pairs
        anatomy_attribute = {}"""
        ### TODO: Extend to include site and accession for dbxrefs.
        
        # TBD: Should this really all run on IRIs?  No


        anat_id = self.anat_iri_gen.generate(start)

        anat_id['label'] = label
        channel_id = self.channel_iri_gen.generate(start)
        channel_id['label']  = label + '_c'

        anatomy_attributes['label'] = label


        self.ni.add_node(labels=['Individual'],
                         IRI=anat_id['iri'],
                         attribute_dict=anatomy_attributes)
        #dataset_short_form = self.ni.nc.commit_list(["MATCH (ds:DataSet) WHERE ds.label = %s RETURN ds.short_form" % dataset])
        self.ew.add_annotation_axiom(s=anat_id[match_on],
                                     r='source',
                                     o=dataset,
                                     match_on=match_on)

        if dbxrefs:
            for db, acc in dbxrefs.items():
                self.ew.add_annotation_axiom(s=anat_id['short_form'],
                                             r='hasDbXref',
                                             o=db,
                                             match_on='short_form',
                                             edge_annotations={'accession': acc}
                                             )

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
                         match_on='short_form')
        return {'channel': channel_id, 'anatomy': anat_id }

    def add_dataSet(self, name, license, pub='',
                    description='', dataset_spec_text='', site=''):

        self.ni.add_node(labels=['Individual', 'DataSet'],
                         IRI=map_iri('data') + name,
                         attribute_dict={
                             'label': name,
                             'short_form': name,
                             'description': description,
                             'dataset_spec_text': dataset_spec_text})
        self.ni.commit()
        self.ew.add_annotation_axiom(s=name,
                                     r='license',
                                     o=license,
                                     match_on='short_form')
        if site:
            self.ew.add_annotation_axiom(s=name,
                                         r='hasDbXref',
                                         o=site,
                                         match_on='short_form')
        if pub:
            self.ew.add_annotation_axiom(s=name,
                                         r='references',
                                         o=pub,
                                         match_on='short_form')





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
