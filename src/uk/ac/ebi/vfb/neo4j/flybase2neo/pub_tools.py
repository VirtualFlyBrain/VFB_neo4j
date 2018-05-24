from .fb_tools import FB2Neo

class pubMover(FB2Neo):
  # STATUS - SKELETON

    def get_pub_details(self, pub_list):
        """Takes list of Fbrfs as input returns ..."""
        query = ""
        return self.query_fb(query)

    def set_pub_details(self, pub_list):
        """Takes list of Fbrfs as input,
        sets these in target Neo DB, returns ... """

        details = self.get_pub_details(pub_list)
        statements = []
        for d in details:
            statements.append("")
        return details

    def get_pub_xrefs(self, pub_list):
        query = ""
        return self.query_fb(query)

    def set_pub_xrefs(self, pub_list):
        xrefs = self.get_pub_xrefs(pub_list)
        statements = []
        for d in xrefs:
            statements.append("")
        return xrefs

    def get_pub_type(self, pub_list):
        query = ""
        return self.query_fb(query)

    def set_pub_type(self, pub_list):
        types = self.get_pub_xrefs(pub_list)
        statements = []
        for d in types:
            statements.append("")
        return types

    def get_related_pubs(self, pub_list):
        query = ""
        return self.query_fb(query)

    def set_related_pubs(self, pub_list):
        rpubs = self.get_pub_xrefs(pub_list)
        statements = []
        for d in rpubs:
            statements.append("")
        return rpubs