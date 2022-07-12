from .fb_tools import FB2Neo
from ...curie_tools import map_iri
import re

class pubMover(FB2Neo):

    def move(self, pub_list):
        self.set_pub_details(pub_list)
        self.set_pub_xrefs_internal(pub_list)
        #self.generate_microref_labels()

    def get_pub_details(self, pub_list):
        """Takes list of Fbrfs as input returns ..."""

        query = "SELECT pub.title as title, pub.miniref as miniref, pub.pyear as year, pub.pages as pages, " \
                "pub.volume as volume, typ.name as type, pub.uniquename as fbrf " \
                "FROM pub " \
                "LEFT OUTER JOIN cvterm typ on typ.cvterm_id = pub.type_id " \
                "WHERE pub.uniquename IN ('%s') " % "', '".join(pub_list)
        return self.query_fb(query)

    def set_pub_details(self, pub_list, commit=True):
        """Takes list of Fbrfs as input,
        sets these in target Neo DB, returns ... """

        details = self.get_pub_details(pub_list)
        for d in details:
            attribute_dict = dict()
            for k in d.keys():
                if d[k] and not (k in ['fbrf', 'type']):
                    attribute_dict[k] = [d[k]]
            if d['miniref']:
                attribute_dict['label'] = d['miniref']
            attribute_dict['self_xref']=['FlyBase']
            self.ni.add_node(labels=['pub', 'Individual'],
                             IRI=map_iri('fb') + d['fbrf'],
                             attribute_dict=attribute_dict)
        if commit:
            self.ni.commit()

    def get_pub_xrefs(self, pub_list):
        query = "SELECT pub.uniquename as fbrf, db.name AS db_name, dbx.accession AS acc FROM pub " \
                "JOIN pub_dbxref pdbx on pdbx.pub_id=pub.pub_id " \
                "JOIN dbxref dbx on pdbx.dbxref_id=dbx.dbxref_id " \
                "JOIN db on dbx.db_id=db.db_id " \
                "WHERE pub.uniquename IN ('%s')" % "', '".join(pub_list)
        return self.query_fb(query)

    def set_pub_xrefs_internal(self, pub_list):
        xrefs = self.get_pub_xrefs(pub_list)
        statements = []
        for d in xrefs:
            if d['db_name'] == 'pubmed':
                statements.append("MATCH (p:pub) WHERE p.short_form = '%s' "
                                  "SET p.PMID = ['%s']" % (d['fbrf'], d['acc']))
            if d['db_name'] == 'PMCID':
                statements.append("MATCH (p:pub) WHERE p.short_form = '%s' "
                                  "SET p.PMCID = ['%s']" % (d['fbrf'], d['acc']))
            if d['db_name'] == 'ISBN':
                statements.append("MATCH (p:pub) WHERE p.short_form = '%s' "
                                  "SET p.PMID = ['%s']" % (d['fbrf'], d['acc']))
            if d['db_name'] == 'DOI':
                statements.append("MATCH (p:pub) WHERE p.short_form = '%s' "
                                  "SET p.DOI = ['%s']" % (d['fbrf'], d['acc']))

        self.nc.commit_list(statements)

    def _generate_pub_xref_cypher(self, pub, db, acc):
        return "MATCH (p:pub), (s:Site) WHERE p.short_form = '%s' " \
                "AND s.label = '%s' " \
                "MERGE (p)-[dbx :hasDbXref]->(s) " \
                "SET dbx.accession = ['%s']" % (pub, db, acc)

    def set_pub_xrefs(self, pub_list):
        xrefs = self.get_pub_xrefs(pub_list)
        s = []
        for d in xrefs:
            s.append(self._generate_pub_xref_cypher(
                pub=d['fbrf'],
                db=d['db_name'],
                acc=d['acc']
            ))
        self.nc.commit_list(s)


    def generate_microref_labels(self):
        ### Needs some work
        self.nc.commit_list(["MATCH (n:pub) where has(n.miniref) SET n.label=[split(n.miniref,',')[0] + ', ' + split(n.miniref,',')[1]]"])


    def get_pub_type(self, pub_list):
        ## Stub
        query = ""
        return self.query_fb(query)

    def set_pub_type(self, pub_list):
        ## Stub
        types = self.get_pub_xrefs(pub_list)
        statements = []
        for d in types:
            statements.append("")
        return types

    def get_related_pubs(self, pub_list):
        ## Stub
        query = ""
        return self.query_fb(query)

    def set_related_pubs(self, pub_list):
        ## Stub
        rpubs = self.get_pub_xrefs(pub_list)
        statements = []
        for d in rpubs:
            statements.append("")
        return rpubs

    def get_authors(self, pub_list):
        query = "SELECT pub.uniquename as fbrf, pa.rank AS rank, " \
                "pa.surname as surname, pa.givennames as givennames, " \
                "a.pubauthor_id as paid FROM pub " \
                "JOIN pubauthor pa on pa.pub_id=pub.pub_id " \
                "WHERE pub.uniquename IN ('%s')" % "', '".join(pub_list)
        return self.query_fb(query)

    def add_authors(self, pub_list):
        ## Stub
        authors = self.get_authors(pub_list)
        statements = ""
        for d in authors:
            statements.append("")
        return
