import unittest
from ..feature_tools import FeatureMover, Feature, split
import sys
import re

class TestFeatureMover(unittest.TestCase):

    def setUp(self):
        self.fm = FeatureMover('http://localhost:7474', 'neo4j', 'neo4j')
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

    def test_gen_split_ep_feat(self):
        s = split(name='MB005B',
                  dbd='FBtp0117486',
                  ad='FBtp0117485',
                  xrefs='VFBsite_FlyLightSplit')
        test = self.fm.gen_split_ep_feat([s])


    def tearDown(self):
        return









