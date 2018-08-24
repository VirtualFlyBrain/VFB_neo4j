import unittest
from ..feature_tools import FeatureMover
import sys
import re

endpoint = sys.argv[1]
usr = sys.argv[2]
pwd = sys.argv[3]
temp_csv_filepath = sys.argv[4]  # Location for readable csv files


class TestFeatureMover(unittest.TestCase):


    def setUp(self):
        self.fm = FeatureMover(endpoint, usr, pwd, temp_csv_filepath)
        # Load up various helper ontologies:
        # FBex
        # SO
        # FBbt

    def test_gp2Gene(self):
        test = self.fm.gp2Gene([''])
        assert len(test) > 0
        assert re.match('FBgn\d{7}', test[0][2])

    def test_gp2allele(self):
        test = self.fm.gp2allele([''])
        assert len(test) > 0
        assert re.match('FBal\d{7}', test[0][2])

    def test_allele2Gene(self):
        test = self.fm.allele2Gene([''])
        assert len(test) > 0
        assert re.match('FBgn\d{7}', test[0][2])

    def test_allele2transgene(self):
        test = self.fm.allele2transgene([''])
        assert len(test) > 0
        assert re.match('FB(tp|ti)\d{7}', test[0][2])

    def test_generate_expression_patterns(self):
        test = self.fm.generate_expression_patterns([''])

    def test_name_synonym_lopokup(self):
        test = self.fm.name_synonym_lookup([''])

    def test_add_features(self):
        test = self.fm.add_features([''])

    def test_add_feature_relations(self):

        bar = self.fm.allele2Gene([''])
        test = self.fm.add_feature_relations(bar)


    def tearDown(self):
        return





