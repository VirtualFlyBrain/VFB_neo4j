#!/usr/bin/env python
from .neo4j_tools import neo4j_connect, results_2_dict_list
from collections import namedtuple
import pandas as pd
import numpy as np


def gen_report(server, query, report_name, column_order=None):
    """Generates a pandas dataframe with
    the results of a cypher query against the
    specified server.
    Args:
        server: server connection as [endpoint, usr, pwd]
        query: cypher query
        report_name: df.name
        column_order: optionally specify column order in df."""
    nc = neo4j_connect(*server)
    r = nc.commit_list([query])
    dc = results_2_dict_list(r)
    report = pd.DataFrame.from_records(dc)
    report.replace(np.nan, '', regex=True, inplace=True)
    report.name = report_name
    if column_order:
        out = report[column_order]
        out.name = report_name
        return out
    else:
        return report


def gen_dataset_report(server,
                       report_name,
                       production_only=False):

    po = ''
    if production_only:
        po = " WHERE ds.production is true "
    return gen_report(
        server,
        query="MATCH (ds:DataSet) with ds " + po +
        "OPTIONAL MATCH (ds)-[:has_reference]->(p:pub) "
        "WITH ds, p "   
        "OPTIONAL MATCH (ds)-[:has_license]->(l:License) "
        "WITH ds, p, l "
        "OPTIONAL MATCH (ds)<-[:has_source]-(i:Individual) " +
        " RETURN ds.short_form, ds.label, ds.production, "
        "l.label as license,  p.short_form as pub, "
        "count(i) as individuals order by ds.short_form",
        report_name=report_name, column_order=['ds.short_form',
                                               'ds.label',
                                               'ds.production',
                                               'pub',
                                               'license',
                                               'individuals'])


def gen_dataset_report_prod(server, report_name):
    return gen_report(
        server,
        query="MATCH (ds:DataSet) WHERE ds.production = True WITH ds "
              "OPTIONAL MATCH (ds)-[:has_reference]->(p:pub) "
              "WITH ds, p "
              "OPTIONAL MATCH (a:Class)-[:has_reference]->(p) "
              "WITH ds, p, count(distinct a) as ontology_terms "
              "OPTIONAL MATCH (:Individual)-[r:overlaps { pub: p.short_form}]->(:Expression_pattern) "
              "WITH ds, p, ontology_terms, COUNT (distinct r) as exp_cur "         
              "OPTIONAL MATCH (ds)-[:has_license]->(l:License) "
              "WITH ds, p, exp_cur, ontology_terms, l "
              "OPTIONAL MATCH (ds)<-[:has_source]-(i:Individual) "
              "RETURN ds.short_form, ds.label,"
              "l.label as license, p.short_form as pub, "
              "count(distinct i) as individuals, exp_cur, ontology_terms "
              "order by ds.short_form",
        report_name=report_name, column_order=['ds.short_form',
                                               'ds.label',
                                               'license',
                                               'pub',
                                               'individuals',
                                               'ontology_terms',
                                               'exp_cur'])



def diff_report(report1: pd.DataFrame, report2: pd.DataFrame):
    """Compare two dataframes. Each dataframe must have a .name attribute.
    Returns a """
    ## see Stack Overflow 36891977
    merged = report1.merge(report2, indicator=True, how='outer')
    left_only = merged[merged['_merge'] == 'left_only']
    right_only = merged[merged['_merge'] == 'right_only']
    out = {report1.name + '_not_' + report2.name: left_only.drop(columns=['_merge']),
             report2.name + '_not_' + report1.name: right_only.drop(columns=['_merge'])}
    return namedtuple('out', out.keys())(*out.values())


def save_report(report, filename):
    report.to_csv(filename, sep='\t', index=False)

def gen_fbbt_report():
    # Stub
    return

def gen_exp_report():
    # Stub
    return

def diff_table():
    # Stub
    return




