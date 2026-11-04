import unittest
from types import SimpleNamespace
from unittest import mock

from uk.ac.ebi.vfb.neo4j.flybase2neo.fb_tools import FB2Neo


def make_fb2neo(commit_list_result):
    """An FB2Neo without the FlyBase / Neo4j connections: only what commit_via_csv touches."""
    fb = object.__new__(FB2Neo)
    fb.ni = SimpleNamespace(statements=[])
    fb.nc = mock.Mock()
    fb.nc.commit_list.return_value = commit_list_result
    return fb


class TestCommitViaCsv(unittest.TestCase):
    """commit_via_csv sends rows as a Cypher parameter (VFB_neo4j#314): no csv, no shared import dir."""

    STATEMENT = "MERGE (n:Class { short_form : line.short_form } ) SET n.label = line.label"

    def test_rows_sent_as_unwind_parameter(self):
        fb = make_fb2neo([{}])
        rows = [{'short_form': 'FBtp1', 'label': 'a'}, {'short_form': 'FBtp2', 'label': 'b'}]
        self.assertTrue(fb.commit_via_csv(self.STATEMENT, rows))
        (statements,), _ = fb.nc.commit_list.call_args
        self.assertEqual(1, len(statements))
        self.assertEqual("UNWIND $rows AS line " + self.STATEMENT, statements[0]['statement'])
        self.assertEqual(rows, statements[0]['parameters']['rows'])

    def test_batches(self):
        fb = make_fb2neo([{}])
        rows = [{'short_form': 'FBtp%d' % i, 'label': 'x'} for i in range(2500)]
        fb.commit_via_csv(self.STATEMENT, rows, batch_size=1000)
        (statements,), _ = fb.nc.commit_list.call_args
        self.assertEqual([1000, 1000, 500], [len(s['parameters']['rows']) for s in statements])

    def test_failed_load_raises(self):
        fb = make_fb2neo(False)  # neo4j_connect.commit_list returns False on any error
        with self.assertRaises(RuntimeError):
            fb.commit_via_csv(self.STATEMENT, [{'short_form': 'FBtp1', 'label': 'a'}])

    def test_commit_false_buffers_and_writes_nothing(self):
        fb = make_fb2neo([{}])
        fb.commit_via_csv(self.STATEMENT, [{'short_form': 'FBtp1', 'label': 'a'}], commit=False)
        fb.nc.commit_list.assert_not_called()
        self.assertEqual(1, len(fb.ni.statements))
        self.assertTrue(fb.ni.statements[0]['statement'].startswith("UNWIND $rows AS line "))

    def test_no_rows_is_a_no_op(self):
        fb = make_fb2neo([{}])
        self.assertTrue(fb.commit_via_csv(self.STATEMENT, []))
        fb.nc.commit_list.assert_not_called()
        self.assertEqual([], fb.ni.statements)


if __name__ == '__main__':
    unittest.main()
