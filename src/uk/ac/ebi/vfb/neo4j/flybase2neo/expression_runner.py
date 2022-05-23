from .feature_tools import FeatureMover, split
from .expression_tools import ExpressionWriter
from ..neo4j_tools import chunks
from .pub_tools import pubMover
import argparse
import pandas as pd
import numpy as np
import pandasql as ps
from sqlalchemy import create_engine
import re
from ..SQL_tools import dict_cursor

import datetime

import warnings

# General strategy:
# 1. Merge on short_form
# 2. For each feature_expression - get features & pubs & FBex
# 3. Generate expression pattern or localization pattern class for each feature
# - depending on if protein or transcript.
# 4. Merge feature & pub + add typing and details.
# 5. Instantiate (merge) related types -> gene for features, adding details

# TODO - Review intermediate datastructures - more alignement and doc needed.

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
    limit = " limit 100 "
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


# def proc_splits(fep_chunk):
#     """Find splits. Modify fep datastructure to incorporate details."""
#     hemidrivers = []
#     fep_chunk['split'] = np.NaN
#     for i, f in fep_chunk.iterrows():
#         if f['comment']:
#             m = re.match("^when combined with @(FB.{9}):(.+)@.*", f['comment'])
#             if m:
#                 f['split'] = {'hemidriver_id': m.group(1),
#                               'hemidriver_name': m.group(2)
#                               }
#                 hemidrivers.append(m.group(1))
#                 m3 = re.match(".+combination referred to as '(.+)'\).*", f['comment'])
#                 if m3:
#                     f['split']['split_combo_id'] = m3.group(1)
#                 m2 = re.match('.*{.*(DBD|AD).*}', m.group(2))
#                 if m2:
#                     f['split']['type'] = m2.group(1)
#                 else:
#                     warnings.warn("Can't identify a type for " + m.group(2))
#     return hemidrivers

def sql_tab_2_df(eng, table_name):
    q = eng.execute("SELECT * FROM %s" % table_name)
    records = dict_cursor(q.cursor)
    return pd.DataFrame.from_records(records)

def proc_splits(eng):

    # Process splits to
    # * Update feature_expression table in temp DB (eng)
    #   * Columns: fbrf, gp, fbex, comment, al, tg, ep, hemidriver
    # * generate a list of split objects to add to PDB
    splitz = [] # Important to collect list by appending to list as set.update(named_tuple) upacks tuple
    # Find all cases where a tg linked to exp_cur statement with a comment
    q = eng.execute("SELECT comment, tg FROM feature_expression "
                    "WHERE tg IS NOT NULL "
                    "AND comment IS NOT NULL"
                    )
    dc = dict_cursor(q.cursor)
    for f in dc:
            syns = tuple() # empty tuple.  tuple, not list, needed for hashing
            # Does assoc comment indicate combination with some other TG
            # If so capture (potential) hemidriver id (m.group(1)) and label (m.group(2))
            m = re.match("^when combined with @(FB.{9}):(.+)@.*", f['comment'])
            # Match synonym for combo (not always present)
            m3 = re.match(".+combination referred to as '(.+)'\).*", f['comment'])
            # Add synonym to syns if present
            if m3:
                syns = (m3.group(1),) # length 1 tuple
            # If combo
            if m:
                # Parse potential hemidriver name to work out if it's a split (AD/DBD), if so, capture which
                m2 = re.match('.*{.*(DBD|AD).*}', m.group(2))
                # If it is a split:
                if m2:
                    hemidriver_id = m.group(1)
                    hemidriver_type = m2.group(1)
                    # link hemidriver to feature_expression table in temp DB
                    eng.execute('UPDATE feature_expression SET hemidriver = "%s" '
                                'WHERE tg = "%s" '
                                'AND comment = "%s" '
                                '' % (hemidriver_id, f['tg'], f['comment']))
                    # If the hemidriver is a DBD:
                    if hemidriver_type == 'DBD':
                        dbd = hemidriver_id
                        ad = f['tg']
                    if hemidriver_type == 'AD':
                        ad = hemidriver_id
                        dbd = f['tg']
                    # Add to list of splits to be added to PDB. dbd = hemidriver; ad = transgene
                    splitz.append(split(dbd=dbd,
                                        ad=ad,
                                        synonyms=syns,
                                        xrefs=tuple()))
                    # Update temp DB to link feature_expression to ep (DBD vs AD defined order FB IDs in ID)
                    eng.execute('UPDATE feature_expression SET ep = "%s" '
                                'WHERE tg = "%s" '
                                'AND comment = "%s" '
                                '' % ('VFBexp_' + dbd + ad,
                                      f['tg'], f['comment']))

                else:
                    warnings.warn("Can't identify AD vs DBD in %s "
                                  "so ignoring this annotation." % m.group(2))
    return set(splitz)


feps = fm.query_fb("SELECT pub.uniquename as fbrf, "
                   "f.uniquename as gp, e.uniquename as fbex, "
                   "fep.value as comment "
                   "FROM feature_expression fe "
                   "JOIN pub ON fe.pub_id = pub.pub_id "
                   "JOIN feature f ON fe.feature_id = f.feature_id "
                   "JOIN expression e ON fe.expression_id = e.expression_id "
                   "LEFT OUTER JOIN feature_expressionprop fep "
                   "ON fe.feature_expression_id = fep.feature_expression_id "
                   "AND fep.type_id = '101625' "
                   "AND fep.value ~ 'when combined with @.*@.*'" 
                   + limit)

# -> chunk results:
# Make lookup with c


print("Processing %d expression statements from FB." % len(feps))

feps_chunked = chunks(feps, 2000)

# * This needs to be modified so that name-synonym lookup is called directly and so is
# avaible to multiple methods. This can be run on case classes, making it easy to plug
# directly into triple-store integration via dipper.


for fep_c in feps_chunked:

    # Using SQLITE for tracking/lookup.  Loading via DataFrame

    # TODO: check whether indexing relevant elements of df will improve performance
    fep_df = pd.DataFrame.from_records(fep_c)
    # The folloging sub is =needed for is NULL queries
    fep_df.replace(to_replace=[None], value=np.NaN, inplace=True)
    extra_columns = ['al', 'tg', 'ep', 'hemidriver']
    for c in extra_columns:
        fep_df[c] = np.NaN
    # Seed SQL DB for tracking
    engine = create_engine('sqlite://', echo=False)
    fep_df.to_sql('feature_expression', con=engine)
    gps = list(fep_df['gp'])
    gp2al = fm.gp2allele(gps)  # A list of triples (as python tuples)
    allele_ids = []
    for t in gp2al:
        engine.execute("UPDATE feature_expression SET al = '%s'"
                       "WHERE gp = '%s'" % (t[2], t[0]))
        allele_ids.append(t[2])
    # Add alleles (starting point for graph).
    if not allele_ids:
        continue
    alleles = fm.add_features(allele_ids)
    # Only add pubs where allele is present
    q = engine.execute("SELECT fbrf from feature_expression WHERE al IS NOT NULL")
    pubs = [i[0] for i in q.fetchall()]
    pm.move(pubs)

    # Add genes linked to alleles (these don't need to be in the table)
    fm.add_feature_relations(fm.allele2Gene(alleles))

    # Find transgenes, add them to table and link them to alleles
    al2tg = fm.allele2transgene(allele_ids)
    tg_ids = []
    for t in al2tg:
        engine.execute("UPDATE feature_expression SET tg = '%s'"
                       "WHERE al = '%s'" % (t[2], t[0]))
        allele_ids.append(t[0])
    transgenes = fm.add_feature_relations(al2tg)  # Dict tg_id: feature object

    # Find and process splits

    # Add regular expression patterns
    q = engine.execute("SELECT tg FROM feature_expression WHERE comment IS NULL AND tg is NOT NULL")
    non_split_tgs = [i[0] for i in q.fetchall()]
    if non_split_tgs:
        eps = fm.generate_expression_patterns(non_split_tgs)
        if eps:
            for e in eps.edges:
                engine.execute("UPDATE feature_expression SET ep = '%s'"
                               "WHERE tg = '%s'" % (e[0], e[2]))

    # TODO - add code to add graph for split hemidriver !
    splits = proc_splits(engine)
    q = engine.execute("SELECT hemidriver from feature_expression WHERE hemidriver IS NOT NULL")
    hemidrivers = [i[0] for i in q.fetchall()] # Just get the function to return or add them!
    fm.add_features(hemidrivers)
    fm.gen_split_ep_feat(splits)

    q = engine.execute("SELECT fbrf, ep, fbex FROM feature_expression "
                       "WHERE ep IS NOT NULL")
    dc = dict_cursor(q.cursor)
    now = datetime.datetime.now()
    print ("Start collecting:")
    print (now.strftime("%Y-%m-%d %H:%M:%S"))
    exp_write = ExpressionWriter(args.endpoint, args.usr, args.pwd)
    exp_write.get_expression([d['fbex'] for d in dc])
    for r in dc:
        exp_write.write_expression(pub=r['fbrf'], ep=r['ep'], fbex=r['fbex'])
    now = datetime.datetime.now()
    print ("Start commit:")
    print (now.strftime("%Y-%m-%d %H:%M:%S"))
    exp_write.commit()
    now = datetime.datetime.now()
    print ("Finished commit:")
    print (now.strftime("%Y-%m-%d %H:%M:%S"))
    exp_write = None
