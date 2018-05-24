from .feature_tools import FeatureMover
from .expression_tools import ExpressionWriter
from ..neo4j_tools import chunks
import sys
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
temp_csv_filepath = sys.argv[4]  # Location for readable csv files

fm = FeatureMover(endpoint, usr, pwd, temp_csv_filepath)
def exp_gen(): return # code for generating and wiring up expression patterns

def add_pubs(pubs): return




# Query feature_expression => pub feature and fbex
feps = fm.query_fb("SELECT pub.uniquename as fbrf, "
                   "f.uniquename as fbid, e.uniquename as fbex "
                   "FROM feature_expression fe "
                   "JOIN pub ON fe.pub_id = pub.pub_id "
                   "JOIN feature f ON fe.feature_id = f.feature_id "
                   "JOIN expression e ON fe.expression_id = e.expression_id limit 1000")

# -> chunk results:

exp_write = ExpressionWriter(endpoint, usr, pwd)

feps_chunked = chunks(feps, 500)


# * This needs to be modified so that name-synonym lookup is called directly and so is
# avaible to multiple methods. This can be run on case classes, making it easy to plug
# directly into triple-store integration via dipper.

for fep_c in feps_chunked:
    #roll lookup.
    lookup = {}
    for x in fep_c:
        if x['fbid'] in lookup.keys():
            lookup[x['fbid']].append(x)
        else:
            lookup[x['fbid']] = [x]

    gene_product_ids = [f['fbid'] for f in fep_c]
    pubs = [f['fbrf'] for f in fep_c]
    taps = [f['fbex'] for f in fep_c]

    #Gene expression
    gp2g = fm.gp2Gene(gene_product_ids)
    gp_lookup = {g[0]: g[2] for g in gp2g}  # Is 1:1 assumption safe?
    expressed_gene_ids = [g[2] for g in gp2g]  # Would be nicer with named tuple
    expressed_genes = fm.add_features(expressed_gene_ids)
    g2ep = fm.generate_expression_patterns(expressed_genes)

    # transgene expression
    gp2al = fm.gp2allele(gene_product_ids)
    tg_allele_ids = [g[2] for g in gp2al]
    al2gp_lookup = {g[2]: g[0] for g in gp2al}  # Is 1:1 assumption safe?

    al2tg = fm.allele2transgene(tg_allele_ids)
    tg_ids = [g[2] for g in al2tg]
    gp_lookup.update({al2gp_lookup[g[0]]: g[2] for g in al2tg})

#    tg_lookup = {g[0]: g[2] for g in gp2tg}
    expressed_transgenes = fm.add_features(tg_ids)
    tg2ep = fm.generate_expression_patterns(expressed_transgenes)

    # Link alleles to genes

    fm.add_features(tg_allele_ids)
    fm.add_feature_relations(al2tg)

    al2g = fm.allele2Gene(tg_allele_ids)
    fm.add_features([g[2] for g in al2g])
    fm.add_feature_relations(al2g)

    # Add FBex
    exp_write.get_expression(FBex_list=taps)

    # better to have function do batch?
    for fe in fep_c:
        exp_write.write_expression(fep_c['pub'],
                                   gp_lookup[fep_c['fbid'],
                                   fep_c['fbex']]) # P

    # Add pubs
    add_pubs(pubs)
    # Shouldn't this store FBex somewhere?  If so, where?  Use OBAN?
    #exp_write.write_fbexp(taps)  # better to wire up to features at same time?

    # Hookup FBex to features, pubs etc.


# Better to do in batch as with everything else?




    
    


