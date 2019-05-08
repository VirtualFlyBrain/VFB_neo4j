from .feature_tools import FeatureMover, split
from .expression_tools import ExpressionWriter
from ..neo4j_tools import chunks
from .pub_tools import pubMover
import argparse
import re
import sys
import pandas as pd
import re

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
                    action="store_true")
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
    limit = " limit 1000"
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

# Critique of approach:
### Currently using DataFrame as an intermediate data structure. This embeds the assumption of 1:1:1 relationships as we go up the graph.  It would be better to use graph directly - maybe by querying back to Neo (in bulk), or by using a Cypher graph datastructure.


# TODO fix find splits.
def find_splits(fep_chunk):
    """Find splits. Modify fep datastructure to incorporate"""
    for f in fep_chunk:
        if f['comment']:
            m = re.match("when combined with @(FB.+):(.+)@ "
                         "\(combination referred to as '(.+)'\)", f['comment'])
            if m:
                f['split'] = {'hemidriver_id': m.group(1),
                              'hemidriver_name': m.group(2),
                              'split_combo_id': m.group(3)}
                m2 = re.match('P{.+(DBD|AD)}', m.group(2))
                f['split']['type'] = m2.group(1)
            else:
                f['split'] = {}


def lookup_2_df(lookup, df: pd.DataFrame, key_column, value_column):
    """Use a dict (lookup) to update a DataFrame, matching key against
    contents of key_column and adding value to value_column.  Value column is
    added to DataFrame if not present."""
    if value_column not in list(df.columns):
        df[value_column] = ''
    for k, v in lookup.items():
        df.loc[df[key_column] == k, value_column] = v


feps = fm.query_fb("SELECT pub.uniquename as fbrf, "
                   "f.uniquename as gp, e.uniquename as fbex, "
                   "fep.value as comment "
                   "FROM feature_expression fe "
                   "JOIN pub ON fe.pub_id = pub.pub_id "
                   "JOIN feature f ON fe.feature_id = f.feature_id "
                   "JOIN expression e ON fe.expression_id = e.expression_id "
                   "LEFT OUTER JOIN feature_expressionprop fep "
                   "ON fe.feature_expression_id = fep.feature_expression_id "
                   "WHERE (fep.type_id = '101625' OR fep.type_id is NULL) " + limit)

find_splits(feps)
# -> chunk results:
# Make lookup with c

fepss = [f for f in feps if f['split']]

print("Processing %d expression statements from FB." % len(feps))

exp_write = ExpressionWriter(args.endpoint, args.usr, args.pwd)

feps_chunked = chunks(fepss, 500)

# * This needs to be modified so that name-synonym lookup is called directly and so is
# avaible to multiple methods. This can be run on case classes, making it easy to plug
# directly into triple-store integration via dipper.


for fep_c in feps_chunked:

    # Using DataFrame for lookup

    fep_df = pd.DataFrame.from_records(fep_c)

    gene_product_ids = [f['gp'] for f in fep_c]
    split_gps = [f['gp'] for f in fep_c if f['split']]
    pubs = [f['fbrf'] for f in fep_c]
    taps = [f['fbex'] for f in fep_c]

    gp_splits = ''

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
    lookup_2_df(df=fep_df, lookup=gp2al_lookup, key_column='gp', value_column='al')
    al2tg = fm.allele2transgene(tg_allele_ids)
    al2tg_lookup = {g[0]: g[2] for g in al2tg}
    lookup_2_df(df=fep_df, lookup=al2tg_lookup, key_column='al', value_column='tg')

    # Add alleles as starting point for graph
    alleles = fm.add_features([f[2] for f in gp2al])

    #  Add feature graph al -> tg -> expression pattern (features are typed as we go)
    transgenes = fm.add_feature_relations(al2tg)
    expressed_transgenes : dict = fm.add_feature_relations(al2tg)

    splits = []
    new_hemidrivers = []
    known_hemidrivers = []
    for i, r in fep_df.iterrows():
        if r['split']:
            new_hemidrivers.append(r['split']['hemidriver_id'])
            expressed_transgenes.pop(r['tg'])
            if r['split']['type'] == 'DBD':
                splits.append(
                    split(name=r['split']['split_combo_id'],
                          dbd=r['split']['id'],
                          ad=r['tg'], xrefs=[]))

            elif r['split']['type'] == 'AD':
                splits.append(
                    split(name=r['split']['split_combo_id'],
                          dbd=r['tg'],
                          ad=r['split']['id'], xrefs=[]))


    fm.add_features(new_hemidrivers)
    split_eps = fm.gen_split_ep_feat(splits=splits)

    ## Should roll split expression patterns in bulk at this point,
    ### This requires being able to track back from transgenes to the original gp.
    ### Rather than make all these lookups - should this whole thing just update fepc?

    # but this requires lookups -> tg
    ##   to populate split objects and pass these in bulk to fm.gen_split_ep method
    ## Also: Watch out for direction! Split objects require DBD vs AD to be distinguished!
    ## I guess this will need to be regex!

    # Roll the last stage of the lookups allowing bridging gp -> ep

    tg2ep_lookup = fm.generate_expression_patterns(expressed_transgenes)

    lookup_2_df(df=fep_df, lookup=tg2ep_lookup, key_column = 'tg', value_column='ep')

    # Link alleles to genes
    genes = fm.add_feature_relations(fm.allele2Gene(alleles.keys()))

    # Roll TAP pdms and store them in exp_write object
    exp_write.get_expression(FBex_list=taps)

### TODO: Refactor loop to use dataFrame.
    # better to have function do batch?
    for fe in fep_c:
        # Not keen on all these nested lookups - but should be safe.
        if fe['gp'] in gp2al_lookup.keys():
            al = gp2al_lookup[fe['gp']]
            if al2tg_lookup:
                if al in al2tg_lookup.keys():
                    tg = al2tg_lookup[al]
                    if tg in tg2ep_lookup.keys():
                        ep = tg2ep_lookup[tg]
                        exp_write.write_expression(pub=fe['fbrf'], ep=ep, fbex=fe['fbex'])  #

    exp_write.commit()
