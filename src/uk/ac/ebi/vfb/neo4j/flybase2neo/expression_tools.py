from uk.ac.ebi.vfb.neo4j.flybase2neo.fb_tools import FB2Neo
from uk.ac.ebi.vfb.curie_tools import map_iri

def expand_stage_range(nc, start, end):
    """nc = neo4j_connect object
    start = start stage (short_form_id string)
    end = end stage (short_form_id string)
    Returns list of intermediate stages.
    """
    stages = [start, end]
    statements = [
        'MATCH p=shortestPath((s:FBDV {short_form:"%s"})<-[:immediately_preceded_by*]-" \
        "(e:FBDV {short_form:"%s"})) RETURN extract(x IN nodes(p) | x.short_form)' % (start, end)]
    r = nc.commit_list(statements)
    stages.append(r[0]['data'][0]['row'][0])
    return stages

class ExpressionWriter(FB2Neo):

    def __init__(self, endpoint, usr, pwd):
        self._init(endpoint, usr, pwd)
        self.FBex_lookup = []

    def get_expression(self, limit=False, FBex_list=False):
        query = 'SELECT c.name as cvt, db.name as cvt_db, dbx.accession as cvt_acc, ec.rank as ec_rank, ' \
                't1.name as ec_type, ectp.value as ectp_value, ' \
                't2.name as ectp_name, ectp.rank as ectp_rank, ' \
                'e.uniquename as fbex ' \
                'FROM expression_cvterm ec ' \
                'JOIN expression e on ec.expression_id=e.expression_id ' \
                'LEFT OUTER JOIN expression_cvtermprop ectp on ec.expression_cvterm_id=ectp.expression_cvterm_id  ' \
                'JOIN cvterm c on ec.cvterm_id=c.cvterm_id  ' \
                'JOIN dbxref dbx ON (dbx.dbxref_id = c.dbxref_id) ' \
                'JOIN db ON (dbx.db_id=db.db_id) ' \
                'JOIN cvterm t1 on ec.cvterm_type_id=t1.cvterm_id  ' \
                'LEFT OUTER JOIN cvterm t2 on ectp.type_id=t2.cvterm_id'

        if FBex_list:
            query += " WHERE e.uniquename in ('%s')" % "','".join(FBex_list)

        query += ' ORDER BY e.uniquename'

        if limit:
            query += " LIMIT %d" % limit


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

# Distinct combos:
#         (anatomy, FROM, operator)
#         (anatomy, OF, operator)
#         (anatomy, TO, operator)
#         (anatomy,, qualifier)
#         (anatomy,,)
#         (assay,,)
#         (cellular, OF, operator)
#         (cellular,, qualifier)
#         (cellular,,)
#         (stage, FROM, operator)
#         (stage, TO, operator)
#         (stage, inter - range, operator)
#         (stage,, operator)
#         (stage,, qualifier)
#         (stage,,)

        exp = self.query_fb(query)

        # make dict keyed on FBex : TAP-like structure

        # Note sure about this structure any more.

        # Structure: { FBex : { start : '', end: '', anatomy: { '' , qualifier) , assay' 'qualifier' }  # Do we need rank?


        def proc_row(ed, out):
            short_form = ed['cvt_db'] + '_' + ed['cvt_acc']
            if ed['ectp_name'] == 'qualifier':
                out['qualifiers'].append(short_form)
            else:
                # Strip out range expansion in FB prod (not reliable)
                if not (d['ectp_value'] == 'inter-range'):
                    out['terms'].append(
                        {'term': short_form, 'operator': d['ectp_value']})

        FBex_lookup = {}
        old_key = ''
        stage, anatomy, cellular, assay = '','','',''
        for d in exp:
            key = d['fbex']
            if not (key == old_key):
                anatomy = {'terms': [], 'qualifiers': []}
                cellular = {'terms': [], 'qualifiers': []}
                stage = {'terms': [], 'qualifiers': []}
                assay = {'terms': [], 'qualifiers': []}
                tap = {'anatomy': anatomy,
                       'cellular': cellular,
                       'stage': stage,
                       'assay': assay}
                FBex_lookup[key] = tap
            if d['ec_type'] == 'stage':
                proc_row(d, stage)
            if d['ec_type'] == 'anatomy':
                proc_row(d, anatomy)
            if d['ec_type'] == 'cellular':
                proc_row(d, cellular)
            if d['ec_type'] == 'assay':
                proc_row(d, assay)
            old_key = key

        self.FBex_lookup = FBex_lookup




        # for d in exp:
        #
        #     qualif
        #     typ = d['ec_type']
        #     FBex_lookup[k]
        #
        #         FBex_lookup[d['fbex']][d['ec_type']][d['ectp_value']] = {}
        #         FBex_lookup[d['fbex']][d['ec_type']][d['ectp_value']].update(
        #                 {"short_form": d['cvt_db'] + '_' + d['cvt_acc'],
        #                  "label": d['cvt'], 'rank1': d['ec_rank'],
        #                  'rank2': d['ectp_rank']})
        #     elif 'anatomy' in d['ec_type']:
        #         if d['ectp_name'] ==  'qualifier' :
        #             FBex_lookup[d['fbex']][d['ec_type']][d['ectp_name']] = {}
        #             FBex_lookup[d['fbex']][d['ec_type']].update(
        #                         {'short_form': d['cvt_db'] + '_' + d['cvt_acc'],
        #                          'label': d['cvt'],
        #                          'rank1': d['ec_rank'],
        #                          'rank2': d['ectp_rank']})
        #         else:
        #             FBex_lookup[d['fbex']][d['ec_type']].update(
        #                         {'short_form': d['cvt_db'] + '_' + d['cvt_acc'],
        #                          'label': d['cvt'],
        #                          'rank1': d['ec_rank']})
        #     elif 'assay' in d['ec_type']:
        #         FBex_lookup[d['fbex']][d['ec_type']].update(
        #                         {'short_form': d['cvt_db'] + '_' + d['cvt_acc'],
        #                          'label': d['cvt'],
        #                          'rank1': d['ec_rank']})

        return FBex_lookup


    def add_anatByStage_node(self, anat, start, end):
        iri = map_iri('vfb')
        short_form = '' # Generic generator?
        label = '%s from %s to %s' % ('', '', '')
        stages = (self.nc, start, end)
        self.ni.add_node()
        self.ew.add_named_subClassOf_ax()
        for s in stages:
            self.ew.add_anon_subClassOf_ax(s=short_form,
                                           r='RO_0002093',  # exists_during
                                           o=s,
                                           match_on=short_form)

        return {'iri': iri, 'short_form': short_form, 'label': label}


    def link_ep2anat(self, a, ep, ad):
        # We need an if clause in here.  Easier to do this with manual addition
        ep_match = "MATCH (ep:Class { short_form: '%s'}), " % ep
        gross_anatomy_match = "(c { short_form: '%s' }) where not('Cell' in labels(c)) " \
                              "MERGE (ep)-[r:overlaps { short_form: '', type: 'Related']->(c) " \
                              "SET properties(r) += %s" % (a, str(ad))
        cell_match_merge = "(c:Class { short_form: '%s' }) where 'Cell' in labels(c) " \
                           "MERGE (ep)-[r:has_part { short_form: '', type: 'Related']->(c) " \
                           "SET properties(r) += %s" % (a, str(ad))
        self.nc.commit_list([ep_match + gross_anatomy_match, ep_match + cell_match_merge])

    def write_fbexp(self, fbex_list):
        for fbex in fbex_list:
            anat = self.add_anatByStage_node()





    def write_expression(self, pub, fbid, fbex):



        # Phase 1 Generate intermediate (stage restricted) anatomy nodes
        # Phase 2 For each FBexp = lookup whether cell or not (closed world assumption)
        # -> Choose has_part vs overlap
        # Add assay and pub on edge.
        # But could consider assoc for this

        ### Where do the different lines get merged?  Do we make a intermediate data structure, or do it all in cypher?
        ### Given that these are already sorted on FBex, couldn't this be done within the loop structure?

        ### Schema for EP
        # https://github.com/VirtualFlyBrain/VFB_neo4j/issues/2
        # (as:Class:Anatomy { "label" :  'lateral horn  - from S-x to S-y'})
        # (as)-[SubClassOf]->(:Anatomy { label:  'lateral horn', short_form: "FBbt_...." })
        # (as)-[exists_during]->(sr:stage { label: 'stage x to y'} )
        # (sr)-[start]->(:stage { label: 'stage x', short_form: 'FBdv_12345678' }
        # (sr)-[end]->(:stage { label: 'stage y', short_form: 'FBdv_22345678' }
        # (ep)-[:overlaps/has_part { assay: '' ; FBex: '' }]->(as) # Could turn this into an OBAN assoc.

        # How to check classification => has_part vs overlaps? Could potentially be a case clause in Cypher.
        # On cell label.  But this means we can't use the usual edge addition method.
        # Instead of doing this in the Cypher, we could use a label match.  This would require a soft fail
        # and ignoring warnings about match failure.

        # Or - could leave out for now and worry about it once in OWL.




        return
