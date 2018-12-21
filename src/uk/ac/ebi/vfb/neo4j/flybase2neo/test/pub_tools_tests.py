import unittest
from ..pub_tools import pubMover
from ...neo4j_tools import results_2_dict_list


class TestPubMover(unittest.TestCase):

    def setUp(self):
        self.pm = pubMover('http://localhost:7475', 'neo4j', 'neo4j')


    def testMove(self):
        self.pm.move(['FBrf0086456', 'FBrf0083714'])
        test = self.pm.nc.commit_list(["MATCH (p:pub { short_form : 'FBrf0086456'}) RETURN p.miniref"])
        if test:
            fu = results_2_dict_list(test)
            assert isinstance(fu[0]['p.miniref'], str)


    def tearDown(self):
        return