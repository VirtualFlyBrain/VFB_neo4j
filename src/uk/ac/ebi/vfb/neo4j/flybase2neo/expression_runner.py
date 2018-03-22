from .feature_tools import FeatureMover
import warnings

# General strategy:
# 1. Merge on short_form
# 2. For each feature_expression - get features & pubs & FBex
# 3. Generate expression pattern or localization pattern class for each feature
# - depending on if protein or transcript.
# 4. Merge feature & pub + add typing and details.
# 5. Instantiate (merge) related types -> gene for features, adding details


endpoint = sys.argv[1]
usr = sys.argv[2]
pwd = sys.argv[3]
temp_csv_filepath = sys.argv[4]

fm = FeatureMover(endpoint, usr, pwd, temp_csv_filepath)
def exp_gen(): return # code for generating and wiring up expression patterns

def add_pubs(): return


# Query feature_expression => pub feature and fbex
feps = fm.query_fb("SELECT pub.uniquename as fbrf, "
                   "f.uniquename as fbid, e.uniquename as fbex "
                   "FROM feature_expression fe "
                   "JOIN pub ON fe.pub_id = pub.pub_id"
                   "JOIN feature f ON fe.feature_id = f.feature_id"
                   "JOIN expression e ON fe.expression_id = e.expression_id")

gene_products = [f['fbid'] for f in feps]
pubs = [f['fbrf'] for f in feps]
taps = [f['fbex'] for f in feps]

# Sketch
# Aim: transform pub:feature:FBex to pub:ep:FBex WHERE ep is expressed as munged gene/ti/tp ID
# Links could be kept in Python, but could stil be efficient with Cypher *if* done as batch
# For every GP
# Follow path from GP -> gene (direct) or GP->allele->ti/tp
# For each in this list:
# Generate expression pattern node + classification & expresses link.

# ID generation:
## FBgn1234567 -> VFBexp_FBgn1234567
## For Anat + stage range nodes - use UUID, or mung FBBt & FBdv Ids, e.g. VFBexp_FBbt_1234567_FBdv_1234567_FBdv_7654321 ?!
## Better than UUIDs as should be quite stable.  Stability allows merge on ID.
## Or could make a hash using combo of IDs.




# TODO - check paths through feature_relations table
fm.add_features(gene_products)
fm.addTypes2Neo(gene_products)
genes = fm.gp2Gene(gene_products)
transgenes = fm.gp2Transgene(gene_products)

fm.add_feature_relations(genes)
fm.add_feature_relations(transgenes)

# Construct gene expression pattern nodes
# Construc transgene expression pattern modes
exp_gen()  # Takes a mapping of gene expression pattern to feature product nodes

add_pubs(pubs)





    
    


