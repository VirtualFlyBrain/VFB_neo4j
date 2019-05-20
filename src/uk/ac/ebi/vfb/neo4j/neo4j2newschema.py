'''
Created on Mar 6, 2017

@author: davidos
'''
import warnings
import re
import json
#import psycopg2
import requests
from uk.ac.ebi.vfb.neo4j.KB_tools import kb_owl_edge_writer
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, results_2_dict_list
from uk.ac.ebi.vfb.neo4j.SQL_tools import get_fb_conn, dict_cursor
#from ..curie_tools import map_iri

edge_writer = kb_owl_edge_writer('http://localhost:7474', 'neo4j', 'neo')

#%load_ext autoreload
#%autoreload 2
s = """MATCH (n)-[r:in_register_with_ro]->(m)
DELETE r"""
edge_writer.nc.commit_list(s)

s = """MATCH (n)-[r:in_register_with]->(m)
CREATE (n)-[r2:in_register_with_ro]->(m) 
SET r2 = r """   
#WITH r 
#DELETE r   
print(s)     
edge_writer.nc.commit_list(s)