from uk.ac.ebi.vfb.neo4j.owl2neo.owl2neo_tools import OWLery2Neo
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--test', help='Run in test mode. ' \
                                   'runs with limits on cypher queries and additions.',
                    action="store_true")
parser.add_argument("neo_endpoint",
                    help="Endpoint for connection to neo4J prod")
parser.add_argument("neo_usr",
                    help="username")
parser.add_argument("neo_pwd",
                    help="password")
parser.add_argument("owlery_endpoint",
                    help="Endpoint for connection to owlery server")
args = parser.parse_args()

label_additions = []
queries = {
    'Nervous_system': "'overlaps' some 'nervous system'",
    'Larval': "'part of' some 'larva'",
    'Adult': "'part of' some 'adult'"
}

o2n = OWLery2Neo(neo=(args.neo_endpoint, args.neo_usr, args.neo_pwd),
                 owlery=args.owlery_endpoint)
o2n.owl_query_2_neo_labels(queries)

