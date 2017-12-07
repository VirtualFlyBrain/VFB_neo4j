'''
Created on Mar 8, 2017

@author: davidos
'''
import unittest
from ..KB_tools import kb_owl_edge_writer, node_importer, gen_id, iri_generator
from ...curie_tools import map_iri
from uk.ac.ebi.vfb.neo4j.neo4j_tools import results_2_dict_list
from pathlib import Path

class TestEdgeWriter(unittest.TestCase):


    def setUp(self):
        self.edge_writer = kb_owl_edge_writer('http://localhost:7474', 'neo4j', 'neo4j')
        s = []
        s.append("MERGE (i1:Individual { iri : 'Aya' }) " \
            "MERGE (r1:Property { iri : 'http://fu.bar/loves', label : 'loves' }) " \
            "MERGE (i2:Individual { iri: 'Freddy' }) ")
        s.append("MERGE (i1:Individual { iri : 'Aya' }) " \
            "MERGE (r1:Property { iri : 'daughter_of' }) " \
            "MERGE (i2:Individual { iri: 'David' }) ")
        s.append("MERGE (s:Class { iri: 'Person' } ) ")
        s.append("MERGE (s:Class { iri: 'Toy' } ) " )                
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
        assert self.edge_writer.test_edge_addition() == False

        
        # Add test of added content?
        
        
    def test_add_anon_type_ax(self):
        pass
    
    def test_add_named_type_ax(self):
        self.edge_writer.add_named_type_ax(s = 'Aya', o = 'Person')
        self.edge_writer.commit()
        assert self.edge_writer.test_edge_addition() == True        
        r1 = self.edge_writer.nc.commit_list(["MATCH (i1:Individual { iri : 'Aya' })-" \
                                              "[r]->" \
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
        self.ni.add_node(labels = ['Individual'], IRI = map_iri('vfb') + "VFB_00000001", 
                         attribute_dict =  { 'short_form' : "VFB_00000001" })
        self.ni.commit()

    
    def test_update_from_obograph(self):
        # Adding this to cope with odd issues with file_path when running python modules on different systems
        p = Path("resources/vfb_ext.json")
        if p.is_file():
            self.ni.update_from_obograph(file_path = "resources/vfb_ext.json")
        else: 
            self.ni.update_from_obograph(file_path = "uk/ac/ebi/vfb/neo4j/test/resources/vfb_ext.json")
        self.ni.commit()
        result = self.ni.nc.commit_list(["MATCH (p:Property) WHERE p.iri = 'http://purl.obolibrary.org/obo/RO_0002350' RETURN p.label as label" ])
        dc = results_2_dict_list(result)
        assert dc[0]['label'] == 'member_of'
        
        result = self.ni.nc.commit_list(["MATCH (p:Class) WHERE p.iri = 'http://purl.obolibrary.org/obo/fbbt/vfb/VFB_10000005' RETURN p.label as label" ])
        dc = results_2_dict_list(result)
        assert dc[0]['label'] == 'cluster'

    
    def tearDown(self):
        self.ni.nc.commit_list(statements = ["MATCH (n) " \
                                             "DETACH DELETE n"])
        
class TestGenId(unittest.TestCase):
    
    def setUp(self):
        self.id_name = {}
        self.id_name['HSNT_00000101'] = 'head'
        self.id_name['HSNT_00000102'] = 'shoulders'
        self.id_name['HSNT_00000103']= 'knees'


    def test_gen_id(self):
        r = gen_id(idp = 'HSNT', ID = 101, length = 8, id_name = self.id_name)
        assert r['short_form'] == 'HSNT_00000104'
        
class TestIriGenerator(unittest.TestCase):
    
    def setUp(self):
        self.ig = iri_generator('http://localhost:7474', 'neo4j', 'neo4j')

    def test_default_id_gen(self):
        self.ig.set_default_config()
        i = self.ig.generate(1)
        assert i['short_form'] == 'VFB_00000002'

if __name__ == "__main__":
    unittest.main()
