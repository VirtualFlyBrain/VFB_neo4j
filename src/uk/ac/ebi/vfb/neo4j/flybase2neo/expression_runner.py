from .feature_tools import FeatureMover
from .expression_tools import ExpressionWriter
from ..neo4j_tools import chunks
from .pub_tools import pubMover
import argparse
import sys
import pandas as pd

import warnings

# General strategy:
# 1. Merge on short_form
# 2. For each feature_expression - get features & pubs & FBex
# 3. Generate expression pattern or localization pattern class for each feature
# - depending on if protein or transcript.
# 4. Merge feature & pub + add typing and details.
# 5. Instantiate (merge) related types -> gene for features, adding details

parser = argparse.ArgumentParser()
parser.add_argument('--test', help='Run in test mode. ' \
                    'runs with limits on cypher queries and additions.',
                    action = "store_true")
parser.add_argument("endpoint",
                    help="Endpoint for connection to neo4J prod")
parser.add_argument("usr",
                    help="username")
parser.add_argument("pwd",
                    help="password")
parser.add_argument("filepath",
                    help="Location for csv files readable by Neo4J.")

args = parser.parse_args()

# endpoint = sys.argv[1]
# usr = sys.argv[2]
# pwd = sys.argv[3]
# temp_csv_filepath = sys.argv[4]  # Location for readable csv files

fm = FeatureMover(args.endpoint, args.usr, args.pwd, args.filepath)
pm = pubMover(args.endpoint, args.usr, args.pwd, args.filepath)

if args.test:
    limit = " limit 100"
else:
    limit = ""


# Query feature_expression => pub feature and fbex

#  General question on splits:
#  Is CHADO actually safe for comments?
## What would happen if we had two on the same pub & feature TAPs that were identical apart from note?  How can these be unique????

#  Plan for splits:
#   Extend query  to pull TAP-specific comments
#   Roll lookup for comments using compound key (pub, feat, FBex)
#   Lookup should values should be parsed comments => partner + combo name.
#   Questions:
#      Do we still need to bridge -> TG
#      At what point in the cycle can we effectively  use this lookup?
#         ??? => EP lookup?

feps = fm.query_fb("SELECT pub.uniquename as fbrf, "
                   "f.uniquename as fbid, e.uniquename as fbex "
                   "FROM feature_expression fe "
                   "JOIN pub ON fe.pub_id = pub.pub_id "
                   "JOIN feature f ON fe.feature_id = f.feature_id "
                   "JOIN expression e ON fe.expression_id = e.expression_id" + limit)

# -> chunk results:
# Make lookup with c

print("Processing %d expression statements from FB." % len(feps))

exp_write = ExpressionWriter(args.endpoint, args.usr, args.pwd)

feps_chunked = chunks(feps, 500)

# * This needs to be modified so that name-synonym lookup is called directly and so is
# avaible to multiple methods. This can be run on case classes, making it easy to plug
# directly into triple-store integration via dipper.

for fep_c in feps_chunked:


    gene_product_ids = [f['fbid'] for f in fep_c]
    pubs = [f['fbrf'] for f in fep_c]
    taps = [f['fbex'] for f in fep_c]

    # Add pub nodes
    pm.move(pubs)

    ## Aims
    # Map TAP to EP
    # Add edges and types to feature graph
    # Add pub nodes

    # Navigating across the feature graph rolling lookups as we go.
    # Path gp -> al -> tg

    gp2al = fm.gp2allele(gene_product_ids)
    if not gp2al:
        continue
    tg_allele_ids = [g[2] for g in gp2al]
    gp2al_lookup = {g[0]: g[2] for g in gp2al}
    al2tg = fm.allele2transgene(tg_allele_ids)
    al2tg_lookup = {g[0]: g[2] for g in al2tg}

    # Add alleles as starting point for graph
    alleles = fm.add_features([f[2] for f in gp2al])

    #  Add feature graph al -> tg -> expression pattern (features are typed as we go)
    transgenes = fm.add_feature_relations(al2tg)
    expressed_transgenes = fm.add_feature_relations(al2tg)

    # Roll the last stage of the lookups allowing bridging gp -> ep
    tg2ep_lookup = fm.generate_expression_patterns(expressed_transgenes)

    # Link alleles to genes
    genes = fm.add_feature_relations(fm.allele2Gene(alleles.keys()))


    # Roll TAP pdms and store them in exp_write object
    exp_write.get_expression(FBex_list=taps)

    # better to have function do batch?
    for fe in fep_c:
        # Not keen on all these nested lookups - but should be safe.
        if fe['fbid'] in gp2al_lookup.keys():
            al = gp2al_lookup[fe['fbid']]
            if al2tg_lookup:
                if al in al2tg_lookup.keys():
                    tg = al2tg_lookup[al]
                    if tg in tg2ep_lookup.keys():
                        ep = tg2ep_lookup[tg]
                        exp_write.write_expression(pub=fe['fbrf'], ep=ep, fbex=fe['fbex'])  #

    exp_write.commit()
