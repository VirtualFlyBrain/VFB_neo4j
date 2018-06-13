from .fb_tools import FB2Neo
from ...curie_tools import map_iri
import re
import warnings
import collections
import uuid

def clean_sgml_tags(sgml_string):
    sgml_string = re.sub('<up>', '[', sgml_string)
    sgml_string = re.sub('</up>', ']', sgml_string)
    sgml_string = re.sub('<down>', '[[', sgml_string)
    sgml_string = re.sub("</down>", ']]', sgml_string)
    return sgml_string

def map_feature_type(fbid, ftype):
    mapping = {'transgenic_transposon': 'SO_0000902',  # Treating as SO: transgene - for consistency with relations
               'insertion_site': 'GENO_0000418', # Treating as inserted transgene - for consistency with relations
                'transposable_element_insertion_site': 'GENO_0000418',  # Treating as inserted transgene
                'natural_transposon_isolate_named': 'SO_0000797',
                'chromosome_structure_variation': 'SO_1000183'
                }

    if ftype == 'gene':
        if re.match('FBal', fbid):
            return 'SO_0001023'
        else:
            return 'SO_0000704 '
    elif ftype in mapping.keys():
        return mapping[ftype]
    else:
        return 'SO_0000110' # Sequence feature


# Using named tuples to standardise immutable objects for holding data.

Feature = collections.namedtuple('Feature', ['symbol',
                                             'fbid',
                                             'synonyms',  # list of synonyms
                                             'iri'
                                             ])

Duple = collections.namedtuple('ftype', ['s', 'o'])
Triple = collections.namedtuple('ftype', ['s', 'r', 'o'])

class FeatureMover(FB2Neo):

    def name_synonym_lookup(self, fbids):
        """Takes a list of fbids, returns a dictionary of Feature objects, keyed on fbid.
        Note - makes unicode name primary.  Makes everything else a synonym"""
        if not fbids:
            warnings.warn("Empty fbid list provided to name_synonym_lookup")
            return False
        # stypes: symbol nickname synonym fullname
        query = "SELECT f.uniquename as fbid, s.name as ascii_name, " \
                "stype.name AS stype, " \
                "fs.is_current, s.synonym_sgml as unicode_name " \
                "FROM feature f " \
                "JOIN feature_synonym fs on (f.feature_id=fs.feature_id) " \
                "JOIN synonym s on (fs.synonym_id=s.synonym_id) " \
                "JOIN cvterm stype on (s.type_id=stype.cvterm_id) " \
                "WHERE f.uniquename IN ('%s')"
        dc = self.query_fb(query % "','".join(fbids))
        results = {}
        old_key = ''
        out = {}
        for d in dc:
            key = d['fbid']
            if not (key == old_key):
                if out:
                    results[old_key] = Feature(**out)
                out = {}
                out['fbid'] = key
                out['iri'] = map_iri('fb') + key
                out['synonyms'] = set()
            if d['stype'] == 'symbol' and d['is_current']:
                out['symbol'] = clean_sgml_tags(d['unicode_name'])
            else:
                out['synonyms'].add(clean_sgml_tags(d['ascii_name']))
                out['synonyms'].add(clean_sgml_tags(d['unicode_name']))
            old_key = key
        return results

    def add_features(self, fbids):
        """Takes a list of fbids, generates a csv and uses this to merge feature nodes,
        adding a unicode label and a list of synonyms.  Returns a dictionary of Feature objects, keyed on fbid"""
        if not fbids:
            warnings.warn("No fbids provided.")
            return False
        feats = self.name_synonym_lookup(fbids)
        proc_names = [f._asdict() for f in feats.values()]
        for d in proc_names:
            d['synonyms'] = '|'.join(d['synonyms'])
        statement = "MERGE (n:Feature:Class { short_form : line.fbid } ) " \
                    "SET n.label = line.symbol SET n.synonyms = split(line.synonyms, '|') " \
                    "SET n.iri = 'http://flybase.org/reports/' + line.fbid"  # Why not using ni? Can kbw have switch to work via csv?
        self.commit_via_csv(statement, proc_names)
        self.addTypes2Neo(fbids)
        return feats

    # Typing

    def grossType(self, fbids):
        query = "SELECT f.uniquename AS fbid, c.name as ftype " \
                "FROM feature f " \
                "JOIN cvterm c on f.type_id=c.cvterm_id " \
                "WHERE f.uniquename in ('%s')" % "','".join(fbids)
        dc = self.query_fb(query)
        results = []
        for d in dc:
            results.append((d['fbid'],
                            map_feature_type(fbid=d['fbid'],
                                             ftype=d['ftype'])))
        return results

    def addTypes2Neo(self, fbids, detail='gross'):
        """Classify FlyBase features identified by a list of fbids.
        Optionally choose detailed classification with detail = 'fine'.
        (This option is currently experimental)."""
        statements = []
        if detail == 'gross':
            types = self.grossType(fbids)
        elif detail == 'fine':
            types = self.fineType(fbids)
        else:
            raise ValueError('detail arg invalid %s' % detail)

        feature_classifications = [{'child': t[0], 'parent': t[1]} for t in types]
        statement = "MATCH (p:Class { short_form: line.parent })" \
                    ",(c:Class { short_form: line.child }) " \
                    "MERGE (p)<-[:SUBCLASSOF]-(c)"
        self.commit_via_csv(statement, feature_classifications)  # This is currently failing, but I have no idea why.  disadvantage csv

    def abberationType(self, abbs):
        """abbs = a list of abberation fbids
        Returns a list of (fbid, type) tuples where type is a SO ID"""
        # Super slow and broken! May not be worth the extra work to fix...
        results = []
        abbs_proc = []  # For tracking processed abbs
        query = "SELECT f.uniquename AS fbid, db.name AS db," \
                "dbx.accession AS acc " \
                "FROM feature f " \
                "JOIN cvterm gross_type ON gross_type.cvterm_id=f.type_id " \
                "JOIN feature_cvterm fc ON fc.feature_id = f.feature_id " \
                "JOIN cvterm fine_type ON fine_type.cvterm_id = fc.cvterm_id " \
                "JOIN feature_cvtermprop fctp ON fctp.feature_cvterm_id = fc.feature_cvterm_id " \
                "JOIN cvterm meta ON meta.cvterm_id = fctp.type_id " \
                "JOIN cvterm gtyp ON gtyp.cvterm_id = f.type_id " \
                "JOIN dbxref dbx ON fine_type.dbxref_id = dbx.dbxref_id " \
                "JOIN db ON dbx.db_id = db.db_id " \
                "WHERE gross_type.name = 'chromosome_structure_variation' -- double checks input gross type" \
                "AND  meta.name = 'wt_class'" \
                "AND f.uniquename in (%s)" % ("'" + "'.'".join(abbs))
        dc = self.query_fb(query)
        for d in dc:
            results.append((d['fbid'], d['db'] + '_' + d['acc']))
            abbs_proc.append(d['fbid'])
        [results.append((a, 'SO_0000110')) for a in abbs if
         a not in abbs_proc]  # Defaulting to generic feature id not abb
        return results

    def fineType(self, fbids):
        gt = self.grossType()
        abbs_list = []
        results = []
        for g in gt:
            if g[1] == '':
                abbs_list.append(g[0])
            else:
                results.append(g)
            results.extend(self.abberationType(abbs_list))

    def _get_objs(self, subject_ids, chado_rel, out_rel, o_idp):
        query_template = "SELECT s.uniquename AS subj, o.uniquename AS obj FROM feature s " \
                         "JOIN feature_relationship fr ON fr.subject_id=s.feature_id " \
                         "JOIN cvterm r ON fr.type_id=r.cvterm_id " \
                         "JOIN feature o ON fr.object_id=o.feature_id " \
                         "WHERE s.uniquename IN ('%s') " \
                         "AND r.name = '%s' " \
                         "AND o.uniquename ~ '%s.+'"
        query = query_template % ("','".join(subject_ids), chado_rel, o_idp)
        dc = self.query_fb(query)
        results = []
        for d in dc:
            results.append((d['subj'], out_rel, d['obj']))
        return results

    def allele2Gene(self, subject_ids):
        """Takes a list of allele IDs, returns a list of triples as python tuples:
         (allele rel gene) where rel is appropriate for addition to prod."""
        return self._get_objs(subject_ids, chado_rel='alleleof', out_rel='GENO_0000408', o_idp='FBgn') # is_allele_of

    # gp - transgene R associated_with Type object by uniquename FBgn
    def gp2allele(self, subject_ids):
        """Takes a list of gene product IDs, returns a list of triples as python tuples:
         (gene_product rel transgene) where rel is appropriate for addition to prod."""
        return self._get_objs(subject_ids, chado_rel='associated_with', out_rel='RO_0002204', o_idp='FBal') # gene_product_of

    # gp - gene associated_with Type object by uniquename FBgn
    def gp2Gene(self, subject_ids):
        """Takes a list of gene product IDs, returns a list of triples as python tuples:
         (gene_product rel gene) where rel is appropriate for addition to prod."""
        return self._get_objs(subject_ids, chado_rel='associated_with', out_rel='RO_0002204', o_idp='FBgn') # gene_product_of

    # transgene - allele  R associated_with Type object by uniquename FBal
    def allele2transgene(self, subject_ids):
        """Takes a list of transgene IDs, returns a list of triples as python tuples:
         (transgene rel allele) where rel is appropriate for addition to prod."""
        return self._get_objs(subject_ids, chado_rel='associated_with', out_rel='GENO_0000408', o_idp='(FBti|FBtp)') # is_allele_of
        # Above treating TG as gene.  This is consistent with ti/tp classified as GENO_0000093 'integrated transgene'
        # See https://github.com/monarch-initiative/ingest-artifacts/blob/2ab4a0835b2717ac2426a2e19f1bd9bedf4d6396/docs/Dipper%20Data%20Model%20cmaps.jpg

    def add_feature_relations(self, triples, assume_subject=True):
        if not assume_subject:
            subjects = [t[0] for t in triples]
            self.add_features(subjects)
            self.addTypes2Neo(subjects)
        objects = [t[2] for t in triples]
        self.add_features(objects)
        self.addTypes2Neo(objects)
        statements = []
        for t in triples:
            self.ew.add_anon_subClassOf_ax(s=t[0],
                                           r=t[1],
                                           o=t[2],
                                           match_on='short_form',
                                           safe_label_edge=True)
#            statements.append(
#                "MATCH (s:Feature { short_form: '%s'}), (o:Feature { short_form: '%s'}) " \
#                "MERGE (s)-[r:%s]->(o)" % (t[0], t[2], t[1])  # Should be using KB_tools (?)
#            )
        self.ew.commit()

    def generate_expression_patterns(self, features):
        if not features:
            return False

        out = {}
        # Keeping this function scope local
        def gen_ep_feat(feat):
            return Feature(iri=map_iri('vfb') + 'VFBexp_' + feat.fbid,
                           fbid='VFBexp_' + feat.fbid,
                           symbol=feat.symbol + ' expression pattern',
                           synonyms=[x + ' expression pattern' for x in feat.synonyms])

        for feat in features.values():
            # Generate iri  - use something derived from FB id as will be 1:1.
            # Use: VFBexp_FBxxnnnnnnn
            ep = gen_ep_feat(feat)
            ad = {'label': ep.symbol, 'synonyms': ep.synonyms}
            out[feat.fbid] = ep.fbid
            # Generate label = 'label . expression pattern'
            # Add node
            self.ni.add_node(labels=['Class'],
                             IRI=ep.iri,
                             attribute_dict=ad)

            self.ew.add_named_subClassOf_ax(s=ep.fbid,
                                            o='CARO_0030002',  # expression pattern
                                            match_on='short_form')
            self.ew.add_anon_subClassOf_ax(s=ep.fbid,
                                           r='RO_0002292',  # expresses
                                           o=feat.fbid,
                                           match_on='short_form')

        self.ni.commit()
        self.ew.commit()
        # Add edges - subClassOf expression pattern; expresses fu (we know fu from the feature list.
        # return iris of expression pattern nodes for further use.  Need link back to original feature ID linked to expression
        return out

