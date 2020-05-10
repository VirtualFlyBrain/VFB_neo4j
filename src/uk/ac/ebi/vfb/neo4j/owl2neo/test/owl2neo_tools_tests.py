import unittest
import warnings
from ..owl2neo_tools import OWLery2Neo
from ...neo4j_tools import results_2_dict_list


class OWLery2NeoTest(unittest.TestCase):
    
    def setUp(self):
        self.o2n = OWLery2Neo(neo=('http://localhost:7475', 'neo4j', 'neo4j'))

    def test_owl_query_2_neo_labels(self):
        queries = {
            'ALG': "'glomerulus' that 'part of' some 'antennal lobe'"
        }
        self.o2n.owl_query_2_neo_labels(queries)
        q = ["MATCH (alg:ALG) return alg limit 1;"]
        qr = self.o2n.nc.commit_list(q)
        if qr:
            self.assertAlmostEquals(results_2_dict_list(qr.items()), 1)
        else:
            warnings.warn("Error '" + q + "' returned no valid results")
if __name__ == '__main__':
    unittest.main()
