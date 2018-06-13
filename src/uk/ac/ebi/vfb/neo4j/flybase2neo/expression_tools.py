from .fb_tools import FB2Neo
from ...curie_tools import map_iri
from ..KB_tools import get_sf
import uuid
from warnings import warn

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
        self.statements = []

    def get_expression(self, limit=0, FBex_list=None):
        """Given a list of FBex IDs, generates a lookup for TAP statements, keyed on FBex.
        Lookup structure:
        Primary keys: stage, anatomy, cellular, assay
        Under each primary key: { qualifiers: [<short_form>,...], terms: { term: '<short_form>', operator: '<short_form>' }
        """
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

        if FBex_list is None: FBex_list = []
        if FBex_list:
            query += " WHERE e.uniquename in ('%s')" % "','".join(FBex_list)

        query += ' ORDER BY e.uniquename'

        if limit:
            query += " LIMIT %d" % limit


#         cvt         |      cvt_db      |                cvt_acc                 | ec_rank | ec_type | ectp_value | ectp_name | ectp_rank |    fbex
# --------------------+------------------+----------------------------------------+---------+---------+------------+-----------+-----------+-------------
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
        stage, anatomy, cellular, assay = '', '', '', ''
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
        return FBex_lookup # Could ditch this.



    def roll_anat_ind(self, tap):
        """Rolls an anatomy node from an FBex"""
        anat_genus = ''
        anat_diff = ''
        stage = ''
        start_stage = ''
        end_stage = ''
        if (len(tap['anatomy']['terms']) == 1):
            anat_genus = tap['anatomy']['terms'][0]['term']
        elif (len(tap['anatomy']['terms']) == 2):
            d = [t['term'] for t in tap['anatomy']['terms'] if t['operator'] == 'OF']
            if d: anat_diff = d[0]
            if anat_diff:
                anat_genus = [t['term'] for t in tap['anatomy']['terms'] if not t['operator'] == 'OF'][0]
            else:
                warn("Don't know how to parse %s" % str(tap))
                return False
        else:
            warn("Don't know how to parse %s" % str(tap))
            return False
        if (len(tap['stage']['terms']) == 1):
            stage = tap['stage']['terms'][0]['term']
        elif (len(tap['stage']['terms']) == 2):
            ss = [t['term'] for t in tap['stage']['terms'] if t['operator'] == 'FROM']
            if ss: start_stage = ss[0]
            es = [t['term'] for t in tap['stage']['terms'] if t['operator'] == 'TO']
            if es: end_stage = es[0]

        iri = map_iri('vfb') + "VFB_internal" + str(uuid.uuid4())
        short_form = get_sf(iri)
        self.ni.add_node(['Individual'], iri)  # No label
        self.ew.add_named_type_ax(s=short_form, o=anat_genus, match_on='short_form')
        if anat_diff:
            self.ew.add_anon_type_ax(s=short_form, r='BFO_0000050', o=anat_diff, match_on='short_form')
        if stage:
            self.ew.add_anon_type_ax(s=short_form, r='RO_0002093', o=stage, match_on='short_form')

        if start_stage:
            self.ew.add_anon_type_ax(s=short_form, r='RO_0002488', o=start_stage, match_on='short_form')
        if end_stage:
            self.ew.add_anon_type_ax(s=short_form, r='RO_0002492', o=end_stage, match_on='short_form')
#        leaving out expansion for now
#        stages = expand_stage_range(self.nc, start_stage, end_stage)

        return {'iri': iri, 'short_form': short_form, 'atype': anat_genus}


    def link_ep2anat(self, a, ep, ad, atype):
        # Need batch checking anatomy labels?
        # Giving up on usual pattern for addition...
        edge_prop_clauses = []
        for k,v in ad.items():
            edge_prop_clauses.append(self.ew._add_textual_attribute('r', k, v))

        ep_match = "MATCH (ep:Class { short_form: '%s'}), (t:Class { short_form: '%s'})" % (ep, atype)
        gross_anatomy_match = ", (a:Individual { short_form: '%s' }) where not('Cell' in labels(t)) " \
                              "MERGE (ep)<-[r:overlaps { short_form: 'RO_0002131', " \
                              "type: 'Related', iri: 'http://purl.obolibrary.org/RO_0002131'}]-(a) " % a


        cell_match_merge = ", (a:Individual { short_form: '%s' }) where 'Cell' in labels(t) " \
                           "MERGE (ep)<-[r:part_of { short_form: 'BFO_0000050', type: 'Related', " \
                            "iri: 'http://purl.obolibrary.org/BFO_0000050'}]-(a) " % a

        self.statements.extend([ep_match + gross_anatomy_match + ' '.join(edge_prop_clauses),
                                ep_match + cell_match_merge + ' '.join(edge_prop_clauses)])


    def write_expression(self, pub, ep, fbex):

        ## This should all switch to OBAN

        a = self.roll_anat_ind(self.FBex_lookup[fbex])
        if a:
            assays = self.FBex_lookup[fbex]['assay']['terms']
            ad = {'pub': pub } # quick and dirty job right now.  Need to switch to OBAN when safe to do so.
            if assays: ad['assay'] = assays[0]['term']
            self.link_ep2anat(a=a['short_form'], ep=ep, ad=ad, atype=a['atype'])


        # MVP:
           # Some link - any link - from expression pattern to anat
           # Worry about stage & stuff later...

        # Phase 1 Generate intermediate (stage restricted) anatomy nodes
        # Phase 2 For each FBexp = lookup whether cell or not (closed world assumption?)
        # -> Choose has_part vs overlap
        # Add assay and pub on edge.
        # But could consider OBAN assoc for this

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

    def commit(self):
        self.ni.commit() # Add new nodes
        self.ew.commit() # Wire up new nodes
        self.nc.commit_list(self.statements) # Make annotation links
        self.statements = []