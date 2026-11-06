"""Offline tests for the loadpdb2m batching changes: node MERGE label, UNWIND row batching, commit retry policy."""
import os
import unittest
from types import SimpleNamespace
from unittest import mock

from uk.ac.ebi.vfb.neo4j.KB_tools import node_importer, kb_owl_edge_writer
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect


def writer(cls):
    w = object.__new__(cls)
    w.statements, w.output, w.log, w.properties, w.triples = [], [], [], {}, {}
    return w


def response(status, body):
    return SimpleNamespace(status_code=status, json=lambda: body, reason='')


class TestAddNodeMergeLabel(unittest.TestCase):
    """MERGE on the uniquely-keyed label and SET the rest, so a node missing one label is matched, not duplicated."""

    def setUp(self):
        os.environ.pop('VFB_NEO4J_UNWIND', None)

    def test_literal_merges_on_class(self):
        ni = writer(node_importer)
        ni.add_node(['Class', 'Expression_pattern'], 'http://virtualflybrain.org/reports/VFBexp_FBtp1')
        self.assertTrue(ni.statements[0].startswith(
            "MERGE (n:Class { iri: 'http://virtualflybrain.org/reports/VFBexp_FBtp1' }) "
            "set n.short_form = 'VFBexp_FBtp1' set n:Entity:Expression_pattern "))

    def test_literal_pub_merges_on_individual(self):
        ni = writer(node_importer)
        ni.add_node(['pub', 'Individual', 'Entity'], 'http://flybase.org/reports/FBrf1')
        self.assertIn("MERGE (n:Individual { iri: ", ni.statements[0])
        self.assertIn("set n:Entity:pub ", ni.statements[0])

    def test_unwind_batches_in_order(self):
        with mock.patch.dict(os.environ, {'VFB_NEO4J_UNWIND': '1', 'VFB_NEO4J_UNWIND_ROWS': '2'}):
            ni = writer(node_importer)
            for i in range(3):
                ni.add_node(['Class', 'Expression_pattern'], 'http://x/VFBexp_%d' % i, {'label': 'l%d' % i})
            ni.add_node(['Individual'], 'http://x/VFB_internal1')
            ni.add_node(['Class', 'Expression_pattern'], 'http://x/VFBexp_9')
        self.assertEqual([2, 1, 1, 1], [len(s['parameters']['rows']) for s in ni.statements])
        self.assertIn("MERGE (n:Class { iri: row.iri })", ni.statements[0]['statement'])
        self.assertIn("MERGE (n:Individual { iri: row.iri })", ni.statements[2]['statement'])
        self.assertEqual({'iri': 'http://x/VFBexp_0', 'short_form': 'VFBexp_0', 'a': {'label': 'l0'}},
                         ni.statements[0]['parameters']['rows'][0])

    def test_param_attributes_match_literal_storage(self):
        ni = writer(node_importer)
        with mock.patch('warnings.warn'):
            a = ni._param_attributes({'s': 'a\\b', 'l': ['a\\b'], 'f': 1.23456789, 'i': 3, 'b': True, 'd': {}})
        self.assertEqual({'s': 'a\\b', 'l': ['a\\\\b'], 'f': 1.234568, 'i': 3, 'b': True}, a)


class TestNamedEdgesUnwind(unittest.TestCase):

    def test_named_axioms_queue_rows(self):
        with mock.patch.dict(os.environ, {'VFB_NEO4J_UNWIND': '1'}):
            ew = writer(kb_owl_edge_writer)
            ew.add_named_subClassOf_ax(s='VFBexp_1', o='CARO_0030002', match_on='short_form')
            ew.add_named_subClassOf_ax(s='VFBexp_2', o='CARO_0030002', match_on='short_form')
            ew.add_named_type_ax(s='VFB_internal1', o='FBbt_1', match_on='short_form')
        self.assertEqual(2, len(ew.statements))
        self.assertIn('MERGE (a)-[e:SUBCLASSOF]->(b)', ew.statements[0]['statement'])
        self.assertEqual(2, len(ew.statements[0]['parameters']['rows']))
        self.assertIn('OPTIONAL MATCH (s:Individual { short_form: row.s })', ew.statements[1]['statement'])
        self.assertIn('MERGE (a)-[e:INSTANCEOF]->(b)', ew.statements[1]['statement'])

    def test_literal_unchanged_without_flag(self):
        os.environ.pop('VFB_NEO4J_UNWIND', None)
        ew = writer(kb_owl_edge_writer)
        ew.add_named_subClassOf_ax(s='VFBexp_1', o='CARO_0030002', match_on='short_form')
        self.assertIsInstance(ew.statements[0], str)


class TestCommitRetryPolicy(unittest.TestCase):

    def nc(self, responses):
        c = object.__new__(neo4j_connect)
        c.base_uri, c.usr, c.pwd, c.commit, c.headers = 'http://x', 'u', 'p', '/tx', {}
        session = mock.Mock()
        session.post.side_effect = responses
        c._session = lambda: session
        return c, session

    @mock.patch('time.sleep')
    @mock.patch('warnings.warn')
    def test_client_error_not_retried(self, *_):
        err = {'errors': [{'code': 'Neo.ClientError.Schema.ConstraintValidationFailed', 'message': 'x'}],
               'results': []}
        c, session = self.nc([response(200, err)] * 3)
        self.assertFalse(c.commit_list(['MERGE (n:Class {iri: "x"})']))
        self.assertEqual(1, session.post.call_count)

    @mock.patch('time.sleep')
    @mock.patch('warnings.warn')
    def test_transient_error_retried(self, *_):
        err = {'errors': [{'code': 'Neo.TransientError.Transaction.DeadlockDetected', 'message': 'x'}],
               'results': []}
        ok = {'errors': [], 'results': [{'columns': [], 'data': []}]}
        c, session = self.nc([response(200, err), response(200, ok)])
        self.assertEqual(ok['results'], c.commit_list(['RETURN 1']))
        self.assertEqual(2, session.post.call_count)

    @mock.patch('time.sleep')
    @mock.patch('warnings.warn')
    def test_gateway_error_retried(self, *_):
        ok = {'errors': [], 'results': []}
        c, session = self.nc([response(502, {}), response(200, ok)])
        self.assertEqual([], c.commit_list(['RETURN 1']))
        self.assertEqual(2, session.post.call_count)


if __name__ == '__main__':
    unittest.main()
