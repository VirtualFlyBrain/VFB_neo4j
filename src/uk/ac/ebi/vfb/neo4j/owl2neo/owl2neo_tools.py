import warnings
from vfb_connect.owl.owlery_query_tools import OWLeryConnect
from ..neo4j_tools import neo4j_connect, chunks
from vfb_connect.cross_server_tools import get_lookup


class OWLery2Neo:

    def __init__(self, neo, owlery=None):
        """
        A wrapper class for querying
        neo = connection details for """
        lookup = get_lookup(limit_by_prefix=['FBbt'], credentials=neo)
        if owlery:    
            self.oc = OWLeryConnect(endpoint=owlery, lookup=lookup)
        else:
            self.oc = OWLeryConnect(lookup=lookup)
        self.nc = neo4j_connect(*neo)

    def owl_query_2_neo_labels(self, queries, query_by_label=True):
        label_additions = []
        for nl, q in queries.items():
            qr = self.oc.get_subclasses(q, query_by_label=query_by_label)
            if qr:
                qr_chunks = chunks(qr, 500)
                for c in qr_chunks:
                    label_additions.append("MATCH (e:Entity) WHERE e.iri IN %s SET e:%s" % (str(c), nl))
            else:
                warnings.warn("No results for '%s' returned" % q)
            # Add in something here to check for query errors
            # Should we be chunking in-clause marches strings?
        self.nc.commit_list(label_additions)
