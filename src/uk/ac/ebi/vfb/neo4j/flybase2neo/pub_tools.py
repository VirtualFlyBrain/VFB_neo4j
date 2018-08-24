from .fb_tools import FB2Neo
import re

class pubMover(FB2Neo):
  # STATUS - IN TESTING

    def move(self, pub_list):
        self.set_pub_details(pub_list)
        self.set_pub_xrefs(pub_list)
        self.generate_microref_labels()

    def get_pub_details(self, pub_list):
        """Takes list of Fbrfs as input returns ..."""

        query = "SELECT pub.title as title, pub.miniref as miniref, pub.pyear as year, pub.pages as pages, " \
                "pub.volume as volume, typ.name as type, pub.uniquename as fbrf, " \
                "db.name AS db_name, dbx.accession AS acc " \
                "FROM pub JOIN cvterm typ on typ.cvterm_id = pub.type_id " \
                "JOIN pub_dbxref pdbx on pdbx.pub_id=pub.pub_id " \
                "JOIN dbxref dbx on pdbx.dbxref_id=dbx.dbxref_id " \
                "JOIN db on dbx.db_id=db.db_id WHERE pub.uniquename IN ('%s') " % "', '".join(pub_list)
        return self.query_fb(query)

    def set_pub_details(self, pub_list):
        """Takes list of Fbrfs as input,
        sets these in target Neo DB, returns ... """

        details = self.get_pub_details(pub_list)
        statements = []
        for d in details:
            if d['title']:
                title = re.sub('"', "\\'", d['title'])
            else:
                title = ''
            statements.append("MERGE (p:pub { FlyBase: '%s' } ) "
                              "SET p.title = \"%s\", p.miniref = \"%s\", "
                              "p.volume = '%s', p.year = '%s', p.pages = '%s'"
                              % (d['fbrf'], title, d['miniref'], d['volume'], d['pyear'], d['pages']))
            # Generate microref from miniref and append as label
        self.nc.commit_list(statements)

    def get_pub_xrefs(self, pub_list):
        query = "SELECT pub.uniquename as fbrf, db.name AS db_name, dbx.accession AS acc FROM pub " \
                "JOIN pub_dbxref pdbx on pdbx.pub_id=pub.pub_id " \
                "JOIN dbxref dbx on pdbx.dbxref_id=dbx.dbxref_id " \
                "JOIN db on dbx.db_id=db.db_id " \
                "WHERE pub.uniquename IN ('%s')" % "', '".join(pub_list)
        return self.query_fb(query)

    def set_pub_xrefs(self, pub_list):
        xrefs = self.get_pub_xrefs(pub_list)
        statements = []
        for d in xrefs:
            if d['db_name'] == 'pubmed':
                statements.append("MATCH (p:pub) WHERE p.FlyBase = '%s' "
                                  "SET p.PMID = '%s'" % (d['fbrf'], d['acc']))
            if d['db_name'] == 'PMCID':
                statements.append("MATCH (p:pub) WHERE p.FlyBase = '%s' "
                                  "SET p.PMCID = '%s'" % (d['fbrf'], d['acc']))
            if d['db_name'] == 'ISBN':
                statements.append("MATCH (p:pub) WHERE p.FlyBase = '%s' "
                                  "SET p.PMID = '%s'" % (d['fbrf'], d['acc']))
            if d['db_name'] == 'DOI':
                statements.append("MATCH (p:pub) WHERE p.FlyBase = '%s' "
                                  "SET p.DOI = '%s'" % (d['fbrf'], d['acc']))

        self.nc.commit_list(statements)


    def generate_microref_labels(self):
        self.nc.commit_list(["MATCH (n:pub) where has(n.miniref) SET n.label=split(n.miniref,',')[0] + ', ' + split(n.miniref,',')[1]"])


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
        query = "SELECT pub.uniquename as fbrf, pa.rank AS rank, pa.surname as surname, pa.givennames as givennames, " \
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

