import unittest
from ..feature_tools import FeatureMover, split, Node
from ...neo4j_tools import results_2_dict_list
import re
import os



class TestFeatureMover(unittest.TestCase):

    def setUp(self):
        self.fm = FeatureMover(os.environ['ENDPOINT'], os.environ['USR'],
                               os.environ['PWD'], os.environ['FILEPATH'])
        # Load up various helper ontologies:
        # VFBext
        # SO
        # FBbt

    def test_gp2Gene(self):
         test = self.fm.gp2Gene(['FBtr0090209'])
         assert len(test) > 0
         assert re.match('FBgn\d{7}', test[0][2])

    def test_gp2allele(self):
        test = self.fm.gp2allele(['FBtr0004716'])
        assert len(test) > 0
        assert re.match('FBal\d{7}', test[0][2])

    def test_allele2Gene(self):
        test = self.fm.allele2Gene(['FBal0040675'])
        assert len(test) > 0
        assert re.match('FBgn\d{7}', test[0][2])

    def test_allele2transgene(self):
        test = self.fm.allele2transgene(['FBal0040675'])
        assert len(test) > 0
        assert re.match('FB(tp|ti)\d{7}', test[0][2])

    def test_generate_expression_patterns(self):
        test = self.fm.generate_expression_patterns([''])


    def test_add_feature_relations(self):

        bar = self.fm.allele2Gene(['FBal0040675'])
        test = self.fm.add_feature_relations(bar)

    def test_name_synonym_lookup(self):
        test = self.fm.name_synonym_lookup(['FBal0040675',
                                            'FBal0040597',
                                            'FBal0028942'])
        assert len(test.keys()) == 3
        assert isinstance(test.pop('FBal0040675'), Node)

    def test_add_features(self):
        test = self.fm.add_features(['FBal0040675',
                                     'FBal0040597',
                                     'FBal0028942'])
        assert len(test.keys()) == 3
        assert isinstance(test.pop('FBal0040675'), Node)
        q = self.fm.nc.commit_list(["MATCH (f:Class:Feature"
                                    "  { short_form: 'FBal0040675'}) "
                                    "RETURN f.short_form"])
        r = results_2_dict_list(q)
        assert r[0]['f.short_form'] == 'FBal0040675'

    def test_gen_split_ep_feat(self):
        s = split(synonyms=['MB005B'],
                  dbd='FBtp0117486',
                  ad='FBtp0117485',
                  xrefs=['VFBsite_FlyLightSplit:MB005B'])
        test = self.fm.gen_split_ep_feat([s])


    def tearDown(self):
        return



if __name__ == "__main__":
    os.environ.get('ENDPOINT', TestFeatureMover.ENDPOINT)
    os.environ.get('USR', TestFeatureMover.USR)
    os.environ.get('PWD', TestFeatureMover.PWD)
    os.environ.get('FILEPATH', TestFeatureMover.FILEPATH)
    unittest.main()




