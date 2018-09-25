import unittest
from ..pub_tools import pubMover


class TestPubMover(unittest.TestCase):

    def setUp(self):
        self.pm = pubMover('http://localhost:7474', 'neo4j', 'neo4j')


    def testMove(self):
        self.pm.move(['FBrf0086456','FBrf0083714'])


    def tearDown(self):
        return