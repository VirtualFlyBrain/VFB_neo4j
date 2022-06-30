'''
Created on Mar 8, 2017

@author: davidos
'''
import unittest
import os

from ..KB_tools import kb_owl_edge_writer, node_importer, gen_id, iri_generator, KB_pattern_writer, EntityChecker, contains_profanity
from ...curie_tools import map_iri
from ..neo4j_tools import results_2_dict_list, neo4j_connect
import re
import time

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
        self.edge_writer = kb_owl_edge_writer('http://localhost:7474', 'neo4j', 'test')
        s = []
        s.append(
            "MERGE (i1:Individual { "
            "iri : 'http://fu.bar/Aya', label: 'Aya', short_form: 'Aya' }) "
            "MERGE (r1:Property { "
            "iri : 'http://fu.bar/loves', label : 'loves', short_form: 'loves'}) "
            "MERGE (i2:Individual { iri: 'http://fu.bar/Freddy' }) ")
        s.append("MERGE (i1:Individual { iri : 'Aya' }) "
            "MERGE (r1:Property { label : 'daughter_of', iri : 'http://fu.bar/dof' }) " 
            "MERGE (i2:Individual { iri: 'http://fu.bar/David', label: 'David' }) ")
        s.append("MERGE (s:Class { iri: 'http://fu.bar/Person', label: 'Person' } ) ")
        s.append("MERGE (s:Class { iri: 'http://fu.bar/awrg', label: 'ice cream' } ) "
                 "MERGE (p:Property { iri: 'http://bar.fu/hsdf', label: 'has license'}) "
                 "MERGE (l:Individual { label: 'Drivers license asdfadf', iri: 'http://a.b.c/asdfr'})")
        self.edge_writer.nc.commit_list(s)
        pass

    def test_add_fact(self):
        # Should probably split these up...

        self.edge_writer.add_fact(s='http://fu.bar/Aya',
                                  r='http://fu.bar/loves',
                                  o='http://fu.bar/Freddy',
                                  match_on='iri',
                                  edge_annotations = {'fu': "ba'r",
                                                      'bin': ['bash', "ba'sh"],
                                                      'i': 1,
                                                      'x': True})
        self.edge_writer.add_fact(s='http://fu.bar/Aya',
                                  r='http://fu.bar/loved',
                                  o='http://fu.bar/Freddy',
                                  match_on='iri'
                                  )

        self.edge_writer.add_fact(s='http://fu.bar/Aya',
                                  r='http://fu.bar/dof',
                                  o='http://fu.bar/David',
                                  match_on='iri',
                                  safe_label_edge=True
                                  )

        self.edge_writer.commit()
        # Checking loves edge
        q = self.edge_writer.nc.commit_list(["MATCH ()-[r { iri : 'http://fu.bar/loves' }]->() return r"])
        r = results_2_dict_list(q)
        # Check new edge made + Property node label copied to new edge
        assert r[0]['r']['label'] == 'loves'
        # Check addition edge attribute from dict
        assert r[0]['r']['x'] is True

        q = self.edge_writer.nc.commit_list(["MATCH ()-[r { iri : 'loved' }]->() return r"])
        r = results_2_dict_list(q)
        assert r == []
        q = self.edge_writer.nc.commit_list(["MATCH ()-[r:daughter_of]->() return r"])
        r = results_2_dict_list(q)
        assert r[0]['r']['label'] == 'daughter_of'


    
    def test_add_named_type_ax(self):
        self.edge_writer.add_named_type_ax(s='Aya',
                                           o='Person',
                                           match_on='label')
        self.edge_writer.commit()
        q = self.edge_writer.nc.commit_list(["MATCH (i1:Individual { label : 'Aya' })-" 
                                              "[r]->" 
                                              "(i2:Class { label: 'Person' } ) "
                                             "RETURN type(r) AS pred"])
        r = results_2_dict_list(q)
        assert r[0]['pred'] == 'INSTANCEOF'

    def test_add_anon_type_ax(self):
        self.edge_writer.add_anon_type_ax(s='Aya',
                                          r='loves',
                                          o='ice cream',
                                          match_on='label')
        self.edge_writer.commit()
        q = self.edge_writer.nc.commit_list(["MATCH (x:Class)<-[r:loves]-(y:Individual) "
                                             "return r.type AS type, x.label AS what, "
                                             "r.label AS rel, y.label AS who"])
        r = results_2_dict_list(q)
        if r:
            assert r[0]['what'] == 'ice cream'
            assert r[0]['type'] == 'Related'
            assert r[0]['rel'] == 'loves'
            assert r[0]['who'] == 'Aya'

        # TODO Add teste here for edge addition + s&o types.


    def test_add_annotation_axiom(self):
        self.edge_writer.add_annotation_axiom(s='David',
                                              r='has license',
                                              o='Drivers license asdfadf',
                                              stype=':Individual',
                                              otype=':Individual',
                                              match_on='label',
                                              safe_label_edge=True)
        self.edge_writer.commit()
        # TODO Add test here for has_license edge type.
        q = self.edge_writer.nc.commit_list(["MATCH (x:Individual { label: 'David'})-[r:has_license]->(y) "
                                             "RETURN type(r) AS rel, r.type AS rtype, "
                                             "x.label AS who"])
        r = results_2_dict_list(q)
        if r:
            assert r[0]['rtype'] == 'Annotation'
            assert r[0]['rel'] == 'has_license'


        
    def tearDown(self):
         s = ["MATCH (n) DETACH DELETE n"]
         self.edge_writer.nc.commit_list(s)
         pass
        
class TestNodeImporter(unittest.TestCase):

    def setUp(self):
        self.ni = node_importer('http://localhost:7474', 'neo4j', 'test')
        ### Maybe need node addition test first?!
        self.ni.add_node(labels = ['Individual'], IRI = map_iri('vfb') + "VFB_00000001")
        self.ni.commit()

    
    def test_update_from_obograph(self):
        # Adding this to cope with odd issues with file_path when running python modules on different systems
        p = get_file_path("uk/ac/ebi/vfb/neo4j/test/resources/vfb_ext.json")
        print(p)
        self.ni.update_from_obograph(file_path=p, include_properties=True)
        self.ni.commit()
        result = self.ni.nc.commit_list(["MATCH (p:Property) WHERE p.iri = 'http://purl.obolibrary.org/obo/RO_0002350' RETURN p.label as label"])
        dc = results_2_dict_list(result)
        assert dc[0]['label'] == 'member_of'
        
        result = self.ni.nc.commit_list(["MATCH (p:Class) WHERE p.iri = 'http://purl.obolibrary.org/obo/fbbt/vfb/VFB_10000005' RETURN p.label as label"])
        dc = results_2_dict_list(result)
        assert dc[0]['label'] == 'cluster'

    def test_update_from_obograph_obsoletes(self):
        p = get_file_path("uk/ac/ebi/vfb/neo4j/test/resources/fbbt-simple-obsoletes.json")
        self.ni.update_from_obograph(file_path=p, include_properties=True)
        self.ni.commit()

        # Check that obsoleted term (MBON01 with term_replaced_by = FBbt:9) is no longer used
        result = self.ni.nc.commit_list(["MATCH (c:Class)<-[]-(i:Individual) "
                                         "WHERE c.iri = 'http://purl.obolibrary.org/obo/FBbt_00100234' "
                                         "RETURN i.short_form"])
        dc = results_2_dict_list(result)
        assert len(dc) == 0

        # Check that replacement term (FBbt:9) is now used
        result = self.ni.nc.commit_list(["MATCH (c:Class)<-[]-(i:Individual) "
                                         "WHERE c.iri = 'http://purl.obolibrary.org/obo/FBbt_9' "
                                         "RETURN i.short_form"])
        dc = results_2_dict_list(result)
        assert len(dc) > 0

        # Check that obsoleted term (MBON02) with consider instead of term_replaced_by is still used
        result = self.ni.nc.commit_list(["MATCH (c:Class)<-[]-(i:Individual) "
                                         "WHERE c.iri = 'http://purl.obolibrary.org/obo/FBbt_00111012' "
                                         "RETURN i.short_form"])
        dc = results_2_dict_list(result)
        assert len(dc) > 0
    
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

    def test_default_id_gen(self):
        start_time = time.time()
        ig = iri_generator('http://localhost:7474', 'neo4j', 'test')
        print("iri_generator init time = " + str(time.time() - start_time))
        start_time = time.time()
        i = ig.generate(1)
        print("ig_generate time = " + str(time.time()-start_time))
        print(i['short_form'])
 #       assert i['short_form'] == 'VFB_00000001'

    def test_base36_id_gen(self):
        start_time = time.time()
        ig_b36 = iri_generator('http://localhost:7474', 'neo4j', 'test', use_base36=True)
        print("b36_iri_generator init time = " + str(time.time() - start_time))
        start_time = time.time()
        print(ig_b36.generate('99999'))
        print(ig_b36.generate('99999'))
        print(ig_b36.generate('jhm00000'))
        print(ig_b36.generate('jhm00000'))
        print("b36_ig_generate time *4 = " + str(time.time()-start_time))

    def test_base36_id_gen_profanity(self):
        start_time = time.time()
        ig_b36 = iri_generator('http://localhost:7474', 'neo4j', 'test', use_base36=True)
        print("b36_iri_generator init time = " + str(time.time() - start_time))
        start_time = time.time()

        assert ig_b36.generate('fucj')["short_form"] == 'VFB_0000fucj'
        assert ig_b36.generate('fucj')["short_form"] == 'VFB_0000fucl'
        assert ig_b36.generate('fucj')["short_form"] == 'VFB_0000fucm'
        assert ig_b36.generate('fucj')["short_form"] == 'VFB_0000fucn'
        print("b36_ig_generate time *4 = " + str(time.time()-start_time))

    def test_profanity_checker(self):
        assert contains_profanity("11fuck02", True) is True
        assert contains_profanity("1h4ndjob", True) is True
        assert contains_profanity("3h4ndj0b", True) is True
        assert contains_profanity("1usuck43", True) is True
        assert contains_profanity("141aassd", True) is True

        assert contains_profanity("e234dsd1", True) is False
        assert contains_profanity("e2none31", True) is False

class TestKBPatternWriter(unittest.TestCase):

    def setUp(self):
        self.nc = neo4j_connect(
            'http://localhost:7474', 'neo4j', 'test')
        self.kpw = KB_pattern_writer(
            'http://localhost:7474', 'neo4j', 'test')
        statements = []
        for k,v in self.kpw.relation_lookup.items():
            short_form = re.split('[/#]', v)[-1]
            statements.append("MERGE (p:Property { iri : '%s', label: '%s', "
                              "short_form : '%s' }) " %
                              (v, k, short_form))

        for k,v in self.kpw.class_lookup.items():
            short_form = re.split('[/#]', v)[-1]
            statements.append("MERGE (p:Class { iri : '%s', "
                              "label: '%s', short_form : '%s' }) " %
                              (v, k, short_form))

        self.nc.commit_list(statements)
        statements = []
        statements.append("MERGE (p:Class { short_form: 'lobulobus', label: 'lobulobus' })")
        statements.append("MERGE (p:Class { short_form: 'brain', label: 'brain' })")
        statements.append("MERGE (p:Property { short_form: 'part_of', label: 'part of', iri: 'http://fubar/part_of' })")
        statements.append("MERGE (p:Property { short_form: 'this_prop_has_no_iri', label: 'this_prop_has_no_iri' })")

        statements.append("MERGE (p:Individual:Template { short_form: 'template_of_dave',"
                          " label: 'template_of_dave' })")
        statements.append("MERGE (s:Site:Individual { short_form : 'fu' }) ")
        statements.append("MATCH (s:Site:Individual { short_form : 'fu' }), "
                          "(i:Individual:Template { short_form: 'template_of_dave'}) "
                          "MERGE (i)-[:hasDbXref { accession: 'GMR_fubar_23'}]->(s)")

        statements.append("MERGE (ds:DataSet:Individual { short_form : 'dosumis2020' }) ")


        self.nc.commit_list(statements)

    def testAddAnatomyImageSet(self):

        #TODO: These should really be split out into named tests with checks of loaded content.

        # General positive test first

        # As above, with test of anon_anatomical_types
        
        t = self.kpw.add_anatomy_image_set(
            dataset='dosumis2020',
            imaging_type='computer graphic',
            label='lobulobus of Dave',
            template='template_of_dave',
            anatomical_type='lobulobus',
            dbxrefs={'fu': 'bar'},
            anon_anatomical_types=([('part_of', 'brain')]),
            start=100
        )
        assert bool(t) is True
        assert bool(self.kpw.commit()) is True

        # Some negative tests

        t = self.kpw.add_anatomy_image_set(
            dataset='asdf',
            imaging_type='computer graphic',
            label='lobulobus of Dave',
            template='asdofiuo',
            anatomical_type='aoiu',
            dbxrefs={'fu': 'bar'},
            start=100,
            hard_fail=False
        )
        assert t is False
        self.kpw.commit()

        t = self.kpw.add_anatomy_image_set(
            dataset='dosumis2020',
            imaging_type='computer graphic',
            label='lobulobus of Dave',
            template='template_of_dave',
            anatomical_type='lobulobus',
            dbxrefs={'fu': 'GMR_fubar_23'},
            start=100,
            hard_fail=False
        )
        assert bool(t) is True

        t = self.kpw.add_anatomy_image_set(
            dataset='dosumis2020',
            imaging_type='computer graphic',
            label='lobulobus of Dave',
            template='template_of_dave',
            anatomical_type='lobulobus',
            dbxrefs={'fu': 'bar'},
            anon_anatomical_types=([('part_of', 'brainz')]),
            start=100,
            hard_fail=False
        )
        assert bool(t) is False

        self.kpw.add_anatomy_image_set(
            dataset='dosumis2020',
            imaging_type='computer graphic',
            label='lobulobus of Dave',
            template='template_of_dave',
            anatomical_type='lobulobus',
            dbxrefs={'fu': 'bar'},
            anon_anatomical_types=([('this_prop_has_no_iri', 'brain')]),
            hard_fail=False,
            start=100
        )
#        assert bool(self.kpw.commit()) is False
        print(self.kpw.commit_log)





    def tearDown(self):
        self.kpw.ni.nc.commit_list(statements=["MATCH (n) "
                                               "DETACH DELETE n"])
        return

class TestEntityChecker(unittest.TestCase):

        def setUp(self):
            s = ["MERGE (i1:Individual { "
                 "iri : 'http://fu.bar/Aya', label: 'Aya', short_form: 'Aya' }) ",
                 "MERGE (s:Site { short_form: 'FlyLight' })",
                 "MATCH (i:Individual { label: 'Aya' }), "
                 "(s:Site { short_form: 'FlyLight' })"
                 " MERGE (i)-[:hasDbXref { accession: 'GMR_fubar_23'}]->(s)"]
            self.ec = EntityChecker('http://localhost:7474', 'neo4j', 'test')
            self.ec.nc.commit_list(s)

        def testEntityCheck(self):
            self.ec.roll_entity_check(labels=['Individual'],
                                      match_on='short_form',
                                      query='Aya')


            self.ec.roll_entity_check(labels=['Individual'],
                                      match_on='iri',
                                      query='http://fu.bar/Aya')

            assert self.ec.check() is True
            # check caching (kinda)
            assert self.ec.roll_entity_check(labels=['Individual'],
                                             match_on='short_form',
                                             query='Aya') is True
            assert 'Aya' in self.ec.cache
            assert len(self.ec.should_exist) == 0


            assert self.ec.check() is True


            self.ec.roll_entity_check(labels=['Individual'],
                                      match_on='label',
                                      query='asdfd')
            # checking failure
            assert self.ec.check() is False

            self.ec.roll_dbxref_check('FlyLight', 'GMR_fubar_23')

            assert self.ec.check() is True

            # Log length should  match number negative tests
            assert len(self.ec.log) == 1

if __name__ == "__main__":
    unittest.main()
