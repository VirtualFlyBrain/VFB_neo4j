import unittest
from ..expression_tools import ExpressionWriter


class TestExpressionWriter(unittest.TestCase):

    def setUp(self):
        self.expw = ExpressionWriter('http://localhost:7475', 'neo4j', 'neo4j')

    def test_get_expression(self):
        self.expw.get_expression(FBex_list=['FBex0000005'])
        assert set(self.expw.FBex_lookup['FBex0000005'].keys()) \
        == {'anatomy', 'stage', 'assay', 'cellular'}

