# Phase 1 - generate
import psycopg2
from neo4j_tools import commit_list, chunks

### Spec:  Need to flesh out ontological status: class vs ind.
# https://github.com/VirtualFlyBrain/VFB_neo4j/issues/2
# (ep:expression_pattern:Class { label :  "expression pattern of X", short_form  : "VFB…." }) 
# (ep)<-[:instanceOf]-(:Individual:Anatomy: { short_form : "VFB_1234567" })
# (ep)-[expresses]->(:genetic_feature {  label : 'p{X}', short_form : "FBbi..." })
# (ep)-[:overlaps { short_form : 'RO_…',  pubs: ["FBrf12343567"]}]
# ->(as:Class:Anatomy { "label" :  'lateral horn  - from S-x to S-y', short_form : 'FBex...', assay: ''})
# (as)-[SubClassOf]->(:Anatomy { label:  'lateral horn', short_form: "FBbt_...." })
# (as)-[during]->(sr:stage { label: 'stage x to y'} )
# (sr)-[start]->(:stage { label: 'stage x', short_form: 'FBdv_12345678' }
# (sr)-[end]->(:stage { label: 'stage y', short_form: 'FBdv_22345678' }

# Stragegy
## Phase 1: Add all classes transgenes/genes for which we have expression data
## Phase 2: Add all expression statements (relying on FBex as unique identifier)
## Phase 3: Link transgenes to expression statement nodes via expression pattern nodes & occurs in edges, adding pubs.
## Phase 4: Link individuals to expression pattern classes.

c = psycopg2.connect(dbname = 'flybase', host ='chado.flybase.org', user = 'flybase')

cursor=c.cursor()

## Phase 1: Add nodes for all transgenes/genes for which we have expression data

cursor.execute("SELECT DISTINCT tgtyp.name as tg_type, obj2.feature_id as transgene_feature_id, obj2.name as transgene_name, " \
"obj2.uniquename as transgene_uniquename, stype.name as gp_type,  " \
"fe.feature_id as gp_feature_id, subj.name as gp_name, subj.uniquename as gp_uname " \
"    FROM feature_expression fe " \
"    JOIN feature subj ON (fe.feature_id=subj.feature_id) " \
"    JOIN cvterm stype ON (subj.type_id=stype.cvterm_id) " \
"    JOIN feature_relationship fr1 ON (fe.feature_id = fr1.subject_id) " \
"    JOIN cvterm rel1 ON (fr1.type_id = rel1.cvterm_id) " \
"    JOIN feature_relationship fr2 ON (fr1.object_id = fr2.subject_id) " \
"    JOIN cvterm rel2 ON (fr2.type_id = rel2.cvterm_id) " \
"    JOIN feature obj2 ON (fr2.object_id = obj2.feature_id) " \
"    JOIN feature_relationship fr3 ON (fr1.object_id = fr3.subject_id) " \
"    JOIN cvterm rel3 ON (fr3.type_id = rel3.cvterm_id) " \
"    JOIN cvterm tgtyp ON (tgtyp.cvterm_id=obj2.type_id) " \
"    WHERE rel1.name='associated_with' " \
"    AND rel2.name='associated_with' " \
"    AND rel3.name='alleleof' " \
"    AND obj2.uniquename ~ 'FBtp|FBti'")



#          name          | transgene_feature_id |  transgene_name   | transgene_uniquename | gp_type_name | gp_feature_id |         gp_name         
# -----------------------+----------------------+-------------------+----------------------+--------------+---------------+-------------------------
#  transgenic_transposon |             40451602 | P{Cyp4d21-GAL4.F} | FBtp0051088          | mRNA         |      61822576 | Scer\GAL4[Cyp4d21.PF]RA
#  transgenic_transposon |             49258075 | P{sNPF-GAL4.V}    | FBtp0069558          | protein      |      61822578 | Scer\GAL4[sNPF.PV]PA

# (ep:expression_pattern:Class { label :  "expression pattern of X", short_form  : "VFB…." }) 
# (ep)-[expresses]->(:genetic_feature {  label : 'p{X}', short_form : "FBbi..." })
# Or include extra node?  This makes it easier to code to, but adds extra complexity to queries.

statements = []
chunked_statements = 
for d in dc:
    ### Better to have general classes for TG, mRNA?
    ### Needs to be chunked
    statements.append( 
    "MERGE (tg:Transgene:%s { name: '%s', short_form: '%s', uri: '%s' }) " \
    "MERGE (gp:GeneProduct:%s { name: '%s', short_form: '%s') " \
    "MERGE (ep:Class:ExpressionPattern { name: 'expression pattern of %s' }) " \
    "MERGE (ep)-[:SubClassof]->(Class:ExpressionPattern { short_form: 'VFB_') " \
    "MERGE (ep)-[:expresses]->(gp)" \
    "MERGE (gp)-[:produced_by]->(tg) " \
    % ( d['tg_type'], d['transgene_name'), d['transgene_uniquename'], base_uri + d['transgene_uniquename'])
    d['gp_type']), d['gp_name'], d['gp_uname'], base_uri + d['gp_uname']))


 --- # Ditto for expressed genes


 # Phase 2 - Add expression statements

"SELECT c.name as cvt, db.name as cvt_db, dbx.accession as cvt_acc, ec.rank as ec_rank, 
t1.name as ec_type, ectp.value as ectp_value, 
t2.name as ectp_name, ectp.rank as ectp_rank,
e.uniquename as fbex
FROM expression_cvterm ec
JOIN expression e on ec.expression_id=e.expression_id
LEFT OUTER JOIN expression_cvtermprop ectp on ec.expression_cvterm_id=ectp.expression_cvterm_id 
JOIN cvterm c on ec.cvterm_id=c.cvterm_id 
JOIN dbxref dbx ON (dbx.dbxref_id = c.dbxref_id)
JOIN db ON (dbx.db_id=db.db_id)
JOIN cvterm t1 on ec.cvterm_type_id=t1.cvterm_id 
LEFT OUTER JOIN cvterm t2 on ectp.type_id=t2.cvterm_id"


#         cvt         |      cvt_db      |                cvt_acc                 | ec_rank | ec_type | ectp_value | ectp_name | ectp_rank |    fbex     
# --------------------+------------------+----------------------------------------+---------+---------+------------+-----------+-----------+-------------
#  embryonic stage 4  | FBdv             | 00005306                               |       0 | stage   |            |           |           | FBex0000001
#  immunolocalization | FlyBase_internal | experimental assays:immunolocalization |       0 | assay   |            |           |           | FBex0000001
#  organism           | FBbt             | 00000001                               |       0 | anatomy |            |           |           | FBex0000001
#  70-100% egg length | FBcv             | 0000132                                |       1 | anatomy |            | qualifier |         0 | FBex0000001
#  embryonic stage 4  | FBdv             | 00005306                               |       0 | stage   |            |           |           | FBex0000002
#  immunolocalization | FlyBase_internal | experimental assays:immunolocalization |       0 | assay   |            |           |           | FBex0000002
#  organism           | FBbt             | 00000001                               |       0 | anatomy |            |           |           | FBex0000002
#  90-100% egg length | FBcv             | 0000139                                |       1 | anatomy |            | qualifier |         0 | FBex0000002
#  embryonic stage 1  | FBdv             | 00005291                               |       0 | stage   | FROM       | operator  |         0 | FBex0000003
#  embryonic stage 5  | FBdv             | 00005311                               |       1 | stage   | TO         | operator  |         0 | FBex0000003

### Where do the different lines get merged?  Do we make a intermediate data structure, or do it all in cypher?
### Given that these are already sorted on FBex, couldn't this be done within the loop structure?

### Spec :
# https://github.com/VirtualFlyBrain/VFB_neo4j/issues/2
# (as:Class:Anatomy { "label" :  'lateral horn  - from S-x to S-y', short_form : 'FBex...', assay: ''})
# (as)-[SubClassOf]->(:Anatomy { label:  'lateral horn', short_form: "FBbt_...." })
# (as)-[during]->(sr:stage { label: 'stage x to y'} )
# (sr)-[start]->(:stage { label: 'stage x', short_form: 'FBdv_12345678' }
# (sr)-[end]->(:stage { label: 'stage y', short_form: 'FBdv_22345678' }

MERGE (x:Class:?? { short_form = '%s' } )  % d['fbex']


statements = []
statements.append("MERGE (x:Class:?? { short_form = '%s' } )  % d['fbex']")

if d['cvt_db'] == 'FBbt':
    statements.append("MATCH (a:Class:Anatomy { short_form: "%s_%s" } ) MERGE (x:Class:?? { short_form = '%s' } ) MERGE (x)-[:SUBCLASSOF]-(a)"  %
                      (d['cvt_db'], d['cvt_acc'], d['fbex']))
if d['cvt_db'] == 'FBdv' and not d['extp_value']:
        statements.append("MATCH (d:Class:Stage { short_form: "%s_%s" } ) MERGE (x:Class:?? { short_form = '%s' } ) MERGE (x)-[:during]-(d)"  %
                      (d['cvt_db'], d['cvt_acc'], d['fbex']))

        # Expansion of stage annotation requires reasoner.  Ignore on first pass?  Or could this be done with a neo4J query?


# Phase 3 - linking expression to features


"SELECT f.name as fname, f.uniquename as funame, e.uniquename as fbex, pub.uniquename as fbrf" \
"FROM feature_expression fe " \
"JOIN feature f ON (f.feature_id=fe.feature_id) " \
"JOIN expression e on (e.expression_id = fe.expression_id) " \
"JOIN pub ON (pub.pub_id=fe.pub_id) "

"MATCH (:GeneProduct { short_form: '%s' )<-[:expresses]-(ep:Expression_pattern), " \
"(fbex:? { short_form = '%s")
"MERGE (ep)-[:overlaps { pub : '%s' , short_form: 'RO_' }]->(fbex) "
% (d['funame'], d['fbex'], d['fbrf'])



        

    