import warnings
from vfb_connect.owl.owlery_query_tools import OWLeryConnect
from ..neo4j_tools import neo4j_connect, chunks, commit_list_in_chunks
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
        label_additions.append("CREATE INDEX ON :Entity(iri)")
        self.nc.commit_list(label_additions)
        for nl, q in queries.items():
            label_additions = []
            print("Adding label '" + nl + "' using query: " + q)
            qr = self.oc.get_subclasses(q, query_by_label=query_by_label)
            if qr:
                print("Results found...")
                qr_chunks = chunks(qr, 100)
                for c in qr_chunks:
                    print("Adding label '" + nl + "' to: " + str(c))
                    label_additions.append("MATCH (e:Entity) WHERE e.iri IN %s SET e:%s" % (str(c), nl))
            else:
                warnings.warn("No results for '%s' returned" % q)
            # Add in something here to check for query errors
            # Should we be chunking in-clause marches strings?
            print("Running label additions for '" + nl)
            commit_list_in_chunks(self.nc, label_additions, verbose=True, chunk_length=100)
            print("Finished label additions for '" + nl)
        
