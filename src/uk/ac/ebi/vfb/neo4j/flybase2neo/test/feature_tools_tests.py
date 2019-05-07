import unittest
from ..feature_tools import FeatureMover, Feature, split
from ...KB_tools import node_importer, results_2_dict_list
import sys
import re

class TestFeatureMover(unittest.TestCase):

    def setUp(self):
        self.fm = FeatureMover('http://localhost:7475', 'neo4j', 'neo4j')
        self.ni = node_importer('http://localhost:7475', 'neo4j', 'neo4j')

        # Load up various helper ontologies:
        # VFBext
        # SO
        # FBbt

#    def test_gp2Gene(self):
#         test = self.fm.gp2Gene([''])
#         assert len(test) > 0
#         assert re.match('FBgn\d{7}', test[0][2])

    # def test_gp2allele(self):
    #    test = self.fm.gp2allele([''])
    #     assert len(test) > 0
    #     assert re.match('FBal\d{7}', test[0][2])

    def test_allele2Gene(self):
        test = self.fm.allele2Gene(['FBal0040675'])
        assert len(test) > 0
        assert re.match('FBgn\d{7}', test[0][2])

    def test_allele2transgene(self):
        test = self.fm.allele2transgene(['FBal0040675'])
        assert len(test) > 0
        assert re.match('FB(tp|ti)\d{7}', test[0][2])

    # def test_generate_expression_patterns(self):
    #     test = self.fm.generate_expression_patterns([''])
    #
    # def test_add_features(self):
    #     test = self.fm.add_features([''])
    #
    # def test_add_feature_relations(self):
    #
    #     bar = self.fm.allele2Gene([''])
    #     test = self.fm.add_feature_relations(bar)

    def test_name_synonym_lookup(self):
        test = self.fm.name_synonym_lookup(['FBal0040675',
                                            'FBal0040597',
                                            'FBal0028942'])
        assert len(test.keys()) == 3
        assert isinstance(test.pop('FBal0040675'), Feature)

    def test_ep(self):
        #self.ni.nc.commit_list([])
        self.ni.update_from_flybase(['FBti0016846'])
        self.ni.commit()
        fu = self.fm.name_synonym_lookup(['FBti0016846'])
        self.fm.generate_expression_patterns(fu)
        q = self.ni.nc.commit_list(["MATCH (ep:Class { short_form: 'VFBexp_%s'})-[]->"
                                    "(f:Class { short_form: '%s'}) "
                                    "return ep, f" % ('FBti0016846',
                                                     'FBti0016846')])
        if q:
            r=results_2_dict_list(q)
            print(r)
            assert len(r) == 1


    def test_gen_split_ep_feat(self):
        s = split(name='MB005B',
                  dbd='FBtp0117486',
                  ad='FBtp0117485',
                  xrefs=['VFBsite_FlyLightSplit:asdasdf'])
        test = self.fm.gen_split_ep_feat([s])


    def tearDown(self):
        to_delete = ['FBtp0117485', 'FBtp0117485', 'FBti0016846', 'VFBexp_FBti0016846',
         'VFBexp_FBtp0117486FBtp0117485']
        self.ni.nc.commit_list(["MATCH (c:Class) WHERE c.short_form in %s DETACH DELETE c"
                                "" % str(to_delete)])
        return









