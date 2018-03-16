'''
Created on Mar 8, 2017

@author: davidos
'''
import unittest

import os
import warnings

from ..KB_tools import kb_owl_edge_writer, node_importer, gen_id, iri_generator, KB_pattern_writer
from ...curie_tools import map_iri
from uk.ac.ebi.vfb.neo4j.neo4j_tools import results_2_dict_list, neo4j_connect
from pathlib import Path
import re

def get_file_path(qualified_path):
    """Takes the fully qualified path of a file as the input.  Checks the current working directory.
    If that directory is within the fully qualified path, returns the local path from working directory
    to file.  If no, returns fully qualified path."""

    # Hacky little workaround for different environments running unit test from different, unpredicatble, directories
    # (PyCharm behavior is particularly odd.)

    pwd = os.getcwd()
    pwdl = pwd.split('/')
    qpl = qualified_path.split('/')
    stat = False
    out = []
    # Scan through qpl until hit lsat entry in pwdl.  Start list from proceeding term.

    for e in qpl:
        if stat: out.append(e)
        if e == pwdl[-1]: stat = 1
    # If nothing in 'out' assume we're at the root of the qualified path.
    if out:
        return '/'.join(out)
    else:
        return qualified_path


class TestEdgeWriter(unittest.TestCase):


    def setUp(self):
        self.edge_writer = kb_owl_edge_writer('http://localhost:7474', 'neo4j', 'neo4j')
        s = []
        s.append("MERGE (i1:Individual { iri : 'Aya' }) "
            "MERGE (r1:Property { iri : 'http://fu.bar/loves', label : 'loves' }) "
            "MERGE (i2:Individual { iri: 'Freddy' }) ")
        s.append("MERGE (i1:Individual { iri : 'Aya' }) "
            "MERGE (r1:Property { iri : 'daughter_of' }) " 
            "MERGE (i2:Individual { iri: 'David' }) ")
        s.append("MERGE (s:Class { iri: 'Person' } ) ")
        s.append("MERGE (s:Class { iri: 'Toy' } ) ")
        self.edge_writer.nc.commit_list(s)
        pass

    def test_add_fact(self):

        self.edge_writer.add_fact(s = 'Aya', r = 'http://fu.bar/loves', 
                                  o = 'Freddy', 
                                  edge_annotations = { 'fu' : "ba'r", 
                                                      'bin': ['bash', "ba'sh"],
                                                      'i' : 1,                                                                                          
                                                      'x' : True })
        assert self.edge_writer.check_proprties() == True
        self.edge_writer.commit() 
        assert self.edge_writer.test_edge_addition() == True  
        self.edge_writer.add_fact(s = 'Aya', r = 'loved', o = 'Freddy', edge_annotations = {} )
        self.edge_writer.commit()
        assert self.edge_writer.test_edge_addition() is False

        
        # Add test of added content?


    
    def test_add_named_type_ax(self):
        self.edge_writer.add_named_type_ax(s = 'Aya', o = 'Person')
        self.edge_writer.commit()
        assert self.edge_writer.test_edge_addition() == True        
        r1 = self.edge_writer.nc.commit_list(["MATCH (i1:Individual { iri : 'Aya' })-" 
                                              "[r]->" 
                                              "(i2:Class { iri: 'Person' } ) RETURN type(r) AS r"])
        assert r1[0]['data'][0]['row'][0] == 'INSTANCEOF'
        
    def tearDown(self):
         # TODO - add some deletions here
         s = ["MATCH (n) DETACH DELETE n"]
         self.edge_writer.nc.commit_list(s)
         pass
        
class TestNodeImporter(unittest.TestCase):

    def setUp(self):
        self.ni = node_importer('http://localhost:7474', 'neo4j', 'neo4j')
        ### Maybe need node addition test first?!
        self.ni.add_node(labels = ['Individual'], IRI = map_iri('vfb') + "VFB_00000001")
        self.ni.commit()

    
    def test_update_from_obograph(self):
        # Adding this to cope with odd issues with file_path when running python modules on different systems
        p = get_file_path("uk/ac/ebi/vfb/neo4j/test/resources/vfb_ext.json")
        print(p)
        self.ni.update_from_obograph(file_path=p)
        self.ni.commit()
        result = self.ni.nc.commit_list(["MATCH (p:Property) WHERE p.iri = 'http://purl.obolibrary.org/obo/RO_0002350' RETURN p.label as label"])
        dc = results_2_dict_list(result)
        assert dc[0]['label'] == 'member_of'
        
        result = self.ni.nc.commit_list(["MATCH (p:Class) WHERE p.iri = 'http://purl.obolibrary.org/obo/fbbt/vfb/VFB_10000005' RETURN p.label as label"])
        dc = results_2_dict_list(result)
        assert dc[0]['label'] == 'cluster'

    
    def tearDown(self):
         self.ni.nc.commit_list(statements=["MATCH (n) "
                                            "DETACH DELETE n"])
        
class TestGenId(unittest.TestCase):
    
    def setUp(self):
        self.id_name = {}
        self.id_name['HSNT_00000101'] = 'head'
        self.id_name['HSNT_00000102'] = 'shoulders'
        self.id_name['HSNT_00000103'] = 'knees'


    def test_gen_id(self):
        r = gen_id(idp = 'HSNT', ID = 101, length = 8, id_name = self.id_name)
        assert r['short_form'] == 'HSNT_00000104'
        
class TestIriGenerator(unittest.TestCase):
    
    def setUp(self):
        self.ig = iri_generator('http://localhost:7474', 'neo4j', 'neo4j')

    def test_default_id_gen(self):
        self.ig.set_default_config()
        i = self.ig.generate(1)
        print(i['short_form'])
        assert i['short_form'] == 'VFB_00000001'

class TestKBPatternWriter(unittest.TestCase):

    def setUp(self):
        self.nc = neo4j_connect(
            'http://localhost:7474', 'neo4j', 'neo4j')
        self.kpw = KB_pattern_writer(
            'http://localhost:7474', 'neo4j', 'neo4j')
        statements = []
        for k,v in self.kpw.relation_lookup.items():
            short_form = re.split('[/#]', v)[-1]
            statements.append("MERGE (p:Property { iri : '%s', label: '%s', short_form : '%s' }) " %
                              (v, k, short_form))

        for k,v in self.kpw.class_lookup.items():
            short_form = re.split('[/#]', v)[-1]
            statements.append("MERGE (p:Class { iri : '%s', label: '%s', short_form : '%s' }) " %
                              (v, k, short_form))

        self.nc.commit_list(statements)
        statements = []

        statements.append("MERGE (p:Class { short_form: 'lobulobus', label: 'lobulobus' })")

        statements.append("MERGE (p:Individual:Template { short_form: 'template_of_dave', label: 'template_of_dave' })")

        statements.append("MERGE (s:Site:Individual { short_form : 'fu' }) ")

        statements.append("MERGE (ds:DataSet:Individual { short_form : 'dosumis2020' }) ")


        self.nc.commit_list(statements)

    def testAddAnatomyImageSet(self):
        self.kpw.add_anatomy_image_set(
            dataset='dosumis2020',
            imaging_type='computer graphic',
            label='lobulobus of Dave',
            template='template_of_dave',
            anatomical_type='lobulobus',
            dbxrefs={'fu': 'bar'},
            start=100
        )
        self.kpw.commit()

    def tearDown(self):
        return





if __name__ == "__main__":
    unittest.main()
