'''
Created on Mar 6, 2017

@author: davidos
'''

iri_map = {'obo': 'http://purl.obolibrary.org/obo/',
           'vfb': 'http://virtualflybrain.org/reports/',
           'fb' : 'http://flybase.org/reports/',
           'data': 'http://virtualflybrain.org/data/'}

def map_iri(curie):
    return iri_map[curie]


