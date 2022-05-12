from .fb_tools import FB2Neo, dict_list_2_dict
from ...curie_tools import map_iri
import re
import pandas as pd
import warnings
from dataclasses import dataclass, field
from typing import List, Dict, Set
import collections


def clean_sgml_tags(sgml_string):
    sgml_string = re.sub('<up>', '[', sgml_string)
    sgml_string = re.sub('</up>', ']', sgml_string)
    sgml_string = re.sub('<down>', '[[', sgml_string)
    sgml_string = re.sub("</down>", ']]', sgml_string)
    return sgml_string


def map_feature_type(fbid, ftype):
    mapping = {'transgenic_transposon': 'SO_0000902',  # Treating as SO: transgene - for consistency with relations
               'insertion_site': 'GENO_0000418',  # Treating as inserted transgene - for consistency with relations
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
        return 'SO_0000110'  # Sequence feature


# Using named tuples to standardise immutable objects for holding data.



Feature = collections.namedtuple('Feature', ['symbol',
                                             'fbid',
                                             'synonyms',  # list of synonyms
                                             'iri'
                                             ])

Duple = collections.namedtuple('ftype', ['s', 'o'])
Triple = collections.namedtuple('ftype', ['s', 'r', 'o'])
split = collections.namedtuple('split', ['synonyms', 'dbd', 'ad', 'xrefs'])


@dataclass
class Node:
    short_form: str
    label: str = ''
    iri: str = ''
    synonyms: Set[str] = field(default_factory=set)
    xrefs: List[str] = field(default_factory=list)


@dataclass
class FeatureRelation:
    """Super-simple graph representation of nodes and edges.
    Keeps feature nodes separate from synthetic (ep) nodes"""
    # Could be more generic having native vs synthetic nodes instead.
    edges: List[Triple] = field(default_factory=list)
    features: Dict[str, Node] = field(default_factory=dict)
    eps: Dict[str, Node] = field(default_factory=dict)



# For splits
# pub FBex tg comment split
# + al
# + tg
# + ep # This must be ep corresponding to tg  +  split  tg so  there is a danger of
# tracking ep based on tg (one hemidriver might be used in multiple - we need a compound
# key.  could be tg; or could keep hemidriver in table.
# How to reconsruct this from nodes and edges?
# short_form IS a compound key! We know one half at the start, and whether it is DBD or AD, but we only get the other half just before the addition.  Solution is to have a separate, bespoke job to update lookup table.  This can happen during Split object generation.






class FeatureMover(FB2Neo):

    def name_synonym_lookup(self, fbids):
        """Takes a list of fbids, returns a dictionary of Feature objects, keyed on fbid.
        Note - makes unicode name primary.  Makes everything else a synonym."""
        # Limitation - no concept of type: name (as opposed to symbol).

        def proc_feature(d, ds):
            # Embedding (nesting) this as not expected to call from outside
            # Should probably check whether this makes code more innefficent.

            if ds:
                out = ds
            else:
                out = Node(short_form=d['fbid'],
                           iri=map_iri('fb') + d['fbid'],
                           label=d['ascii_name'],
                           synonyms=set())
            if d['stype'] == 'symbol' and d['is_current']:
                out.label = clean_sgml_tags(d['unicode_name'])
                out.synonyms.add(clean_sgml_tags(d['ascii_name']))
            else:
                out.synonyms.add(clean_sgml_tags(d['ascii_name']))
                out.synonyms.add(clean_sgml_tags(d['unicode_name']))
            return out

        if not fbids:
            warnings.warn("Empty fbid list provided to name_synonym_lookup")
            return False
        # stypes: symbol nickname synonym fullname
        query = "SELECT f.uniquename as fbid, s.name as ascii_name, " \
                "stype.name AS stype, " \
                "fs.is_current, s.synonym_sgml as unicode_name " \
                "FROM feature f " \
                "LEFT OUTER JOIN feature_synonym fs on (f.feature_id=fs.feature_id) " \
                "JOIN synonym s on (fs.synonym_id=s.synonym_id) " \
                "JOIN cvterm stype on (s.type_id=stype.cvterm_id) " \
                "WHERE f.uniquename IN ('%s') ORDER BY fbid"
        dc = self.query_fb(query % "','".join(fbids))

        return dict_list_2_dict(key='fbid', dict_list=dc, pfunc=proc_feature, sort=False)

    def add_features(self, fbids, commit=True):
        """Takes a list of fbids, generates a csv and uses this to merge feature nodes,
        adding a unicode label and a list of synonyms.  Returns a dictionary of Feature objects, keyed on fbid"""
        if not fbids:
            warnings.warn("No fbids provided.")
            return False
        feats = self.name_synonym_lookup(fbids)
        proc_names = [f.__dict__ for f in feats.values()]

        for d in proc_names:
            d['synonyms'] = '|'.join(d['synonyms'])
        statement = "MERGE (n:Class { short_form : line.short_form } ) " \
                    "SET n.label = line.label SET n.synonyms = split(line.synonyms, '|') " \
                    "SET n.iri = 'http://flybase.org/reports/' + line.short_form " \
                    "SET n:Feature SET n.self_xref = True"

        if commit:
            self.commit_via_csv(statement, proc_names)
        self.addTypes2Neo(fbids=fbids, commit=commit)
        return feats

    def feature_robot_template(self, fbids):
        """Takes a list of FBids, looks up info (via name_synonym_lookup) and makes a robot template."""
        feature_details = self.name_synonym_lookup(fbids)

        template_seed = collections.OrderedDict([('iri', 'ID'), ("label", "A rdfs:label"),
                                                 ("synonyms", "A oboInOwl:hasExactSynonym SPLIT=|")])
        template = pd.DataFrame.from_records([template_seed])
        for f in fbids:
            row_od = collections.OrderedDict([])  # new template row as an empty ordered dictionary
            for c in template.columns:  # make columns and blank data for new template row
                row_od.update([(c, "")])
            row_od["iri"] = feature_details[f].iri
            row_od["label"] = feature_details[f].label
            row_od["synonyms"] = '|'.join(feature_details[f].synonyms)
            new_row = pd.DataFrame.from_records([row_od])
            template = pd.concat([template, new_row], ignore_index=True, sort=False)
        template.to_csv(self.file_path + "template.tsv", sep="\t", header=True, index=False)


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

    def addTypes2Neo(self, fbids, detail='gross', commit=True):
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
        if commit:
            self.commit_via_csv(statement,
                                feature_classifications)

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
                         "AND o.uniquename ~ '%s.+'" \
                         "AND NOT o.is_obsolete"
        query = query_template % ("','".join(subject_ids), chado_rel, o_idp)
        dc = self.query_fb(query)
        results = []
        for d in dc:
            results.append((d['subj'], out_rel, d['obj']))
        return results

    def allele2Gene(self, subject_ids):
        """Takes a list of allele IDs, returns a list of triples as python tuples:
         (allele rel gene) where rel is appropriate for addition to prod."""
        return self._get_objs(subject_ids, chado_rel='alleleof',
                              out_rel='GENO_0000408', o_idp='FBgn')  # is_allele_of

    # gp - transgene R associated_with Type object by uniquename FBgn
    def gp2allele(self, subject_ids):
        """Takes a list of gene product IDs, returns a list of triples as python tuples:
         (gene_product rel transgene) where rel is appropriate for addition to prod."""
        return self._get_objs(subject_ids, chado_rel='associated_with', out_rel='RO_0002204',
                              o_idp='FBal')  # gene_product_of

    # gp - gene associated_with Type object by uniquename FBgn
    def gp2Gene(self, subject_ids):
        """Takes a list of gene product IDs, returns a list of triples as python tuples:
         (gene_product rel gene) where rel is appropriate for addition to prod."""
        return self._get_objs(subject_ids, chado_rel='associated_with', out_rel='RO_0002204',
                              o_idp='FBgn')  # gene_product_of

    # transgene - allele  R associated_with Type object by uniquename FBal
    def allele2transgene(self, subject_ids):
        """Takes a list of transgene IDs, returns a list of triples as python tuples:
         (transgene rel allele) where rel is appropriate for addition to prod."""
        return self._get_objs(subject_ids, chado_rel='associated_with', out_rel='BFO_0000051',
                              o_idp='(FBti|FBtp)')  # Hard to choose a relation for this - have gone with has_part - dipper schema docs not much help.
        # See https://github.com/monarch-initiative/ingest-artifacts/blob/2ab4a0835b2717ac2426a2e19f1bd9bedf4d6396/docs/Dipper%20Data%20Model%20cmaps.jpg

    def add_feature_relations(self, triples, assume_subject=True, commit=True):
        """Takes a list of triples as python tuples.
        If assume_subject is False - adds subject
        Adds object
        Adds relationship
        RETURNS a dict object_short_form: feature object"""

        if not assume_subject:
            subjects = [t[0] for t in triples]
            self.add_features(subjects)
            self.addTypes2Neo(subjects)
        objects = [t[2] for t in triples]
        if not objects:
            warnings.warn("No legal triples passed to add feature relations.")
            return False
        objects_pdm = self.add_features(objects)
        self.addTypes2Neo(objects)
        for t in triples:
            self.ew.add_anon_subClassOf_ax(s=t[0],
                                           r=t[1],
                                           o=t[2],
                                           match_on='short_form',
                                           safe_label_edge=True)

        if commit:
            self.ew.commit()
        return FeatureRelation(features=objects_pdm, edges=triples)

    def generate_expression_patterns(self, features, feature_objects=False, add_features =True, commit=True):


        """Takes a list of features as input,
        generates expression patterns for these features.
        returns an dict of ep_feat_short_form: Feature object
        By default, features = a list of short_form ids (FBids)
        If feature_objects=True, then features = a dict of feature objects keyed on short_form
        (This is useful for when a dict of feature objects is already available).
         If add_features = True, then features are added before the ep (making this false is useful for speed)
         Method will only commit if commit = true othewise calling script must commit.  Useful for batching:
            self.ni.commit(batch = ??)
            self.ew.commit(batch = ??)
        """

        if not features:
            warnings.warn("No features provided.")
            return False


        eps = {}
        triples = []

        if not feature_objects:
            if add_features:
                features = self.add_features(features)
            else:
                features = self.name_synonym_lookup(features)

        # Keeping this function scope local
        def gen_ep_feat(feat):
            return Node(iri=map_iri('vfb') + 'VFBexp_' + feat.short_form,
                        short_form='VFBexp_' + feat.short_form,
                        label=feat.label + ' expression pattern',
                        synonyms=[x + ' expression pattern' for x in feat.synonyms.split('|')])

        for feat in features.values():
            # Generate iri  - use something derived from FB id as will be 1:1.
            # Use: VFBexp_FBnnnnnnn
            ep = gen_ep_feat(feat)
            ad = {'label': ep.label, 'synonyms': ep.synonyms}
            eps[ep.short_form] = ep,


            # Generate label = 'label . expression pattern'
            # Add node

            self.ni.add_node(labels=['Class'],
                             IRI=ep.iri,
                             attribute_dict=ad)

            self.ew.add_named_subClassOf_ax(s=ep.short_form,
                                            o='CARO_0030002',  # expression pattern
                                            match_on='short_form')

            t = Triple(s=ep.short_form, r='RO_0002292', o=feat.short_form)
            self.ew.add_anon_subClassOf_ax(s=t.s,
                                           r=t.r,  # expresses
                                           o=t.o,
                                           match_on='short_form',
                                           safe_label_edge=True)
            triples.append(t)

        if commit:
            self.ni.commit()
            self.ew.commit()

        # Add edges - subClassOf expression pattern; expresses fu (we know fu from the feature list.
        # return iris of expression pattern nodes for further use.  Need link back to original feature ID linked to expression

        # Better standardise output here?
        return FeatureRelation(features=features, edges=triples, eps=eps)


    def gen_split_ep_feat(self, splits, add_feats = True, add_feature_details=False, commit=True):
        """Adds split expression pattern nodes to Neo following
        Returns a dict of feature objects keyed on
        schema: (sep)-[:has_hemidriver]->(construct).
        args:
        splits: An array of split objects, namedtuple['synonyms', 'dbd', 'ad', 'xrefs']
        Returns dict
        """

        out = {}
        for s in splits:
            if add_feats:
                if add_feature_details:
                    feats = self.add_features([s.ad, s.dbd])
                else:
                    feats = self.name_synonym_lookup([s.ad, s.dbd])
                    for k, v in feats.items():
                        self.ni.add_node(labels=['Class', 'Feature'],
                                         IRI=map_iri('fb') + k,
                                         attribute_dict={'label': v.label})
            else:
                feats = self.name_synonym_lookup([s.ad, s.dbd])


            short_form = 'VFBexp_' + s.dbd + s.ad
            iri = map_iri('vfb') + short_form
            ad = {'label' : feats[s.dbd].label + ' ∩ ' +
                  feats[s.ad].label +' expression pattern',
                  'synonyms': s.synonyms,
                  'description': ['The sum of all cells at the intersection between '
                                   'the expression patterns of %s and'
                                   ' %s.' % (feats[s.dbd].label,
                                             feats[s.ad].label)]}


            out[short_form] = {'attributes': ad, 'iri': iri,
                               short_form: short_form, 'xrefs': s.xrefs}

            for x in s.xrefs:
                self.ew.add_xref(s=short_form,
                                 xref=x,
                                 stype=':Class')

            self.ni.add_node(labels=['Class'],
                             IRI=iri,
                             attribute_dict=ad)

            self.ew.add_named_subClassOf_ax(s=short_form,
                                            o='VFBext_0000010',
                                            match_on='short_form')

            self.ew.add_anon_subClassOf_ax(s=short_form,
                                           r='VFBext_0000008',
                                           o=s.ad,
                                           match_on='short_form')
            self.ew.add_anon_subClassOf_ax(s=short_form,
                                           r='VFBext_0000008',
                                           o=s.dbd,
                                           match_on='short_form')



        if commit:
            self.ni.commit()
            self.ew.commit()
        return out







