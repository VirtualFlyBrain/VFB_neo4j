import unittest
from unittest import mock

import base36

from uk.ac.ebi.vfb.neo4j.KB_tools import iri_generator, KB_pattern_writer


def result(ids):
    return [{'columns': ['short_form', 'label'], 'data': [{'row': [i, 'x']} for i in ids]}]


def make_gen(responses, idp='VFB'):
    with mock.patch('uk.ac.ebi.vfb.neo4j.KB_tools.neo4j_connect'):
        g = iri_generator('http://kb', 'u', 'p', use_base36=True)
    if idp == 'VFBc':
        g.set_channel_config()
    g.nc = mock.Mock()
    g.nc.commit_list.side_effect = list(responses)
    return g


class TestLoadExisting(unittest.TestCase):

    def test_failed_load_raises_and_mints_nothing(self):
        g = make_gen([False])                      # commit_list returns False after its retries
        with self.assertRaises(RuntimeError):
            g.generate('100000')
        self.assertFalse(g._loaded)

    def test_failed_load_is_retried(self):
        g = make_gen([False, result(['VFB_00100000'])])
        with self.assertRaises(RuntimeError):
            g.generate('100000')
        self.assertEqual('VFB_00100001', g.generate('100000')['short_form'])
        self.assertEqual(2, g.nc.commit_list.call_count)

    def test_existing_and_reserved_ids_are_skipped(self):
        g = make_gen([result(['VFB_00100000', 'VFB_00100001'])])   # the UNION returns individuals and reservations
        self.assertEqual('VFB_00100002', g.generate('100000')['short_form'])
        (statements,), _ = g.nc.commit_list.call_args
        self.assertIn('IdReservation', statements[0])

    def test_loaded_once(self):
        g = make_gen([result([])])
        g.generate('100000')
        g.generate('100000')
        self.assertEqual(1, g.nc.commit_list.call_count)


class TestReserveImageIds(unittest.TestCase):

    def make_writer(self, anat_ids, chan_ids, reserve_response=None):
        with mock.patch('uk.ac.ebi.vfb.neo4j.KB_tools.neo4j_connect'):
            pw = KB_pattern_writer('http://kb', 'u', 'p', use_base36=True)
        # every connection is the same patched mock: give each its own
        pw.anat_iri_gen.nc, pw.channel_iri_gen.nc, pw.ni.nc = mock.Mock(), mock.Mock(), mock.Mock()
        pw.anat_iri_gen.nc.commit_list.side_effect = [result(anat_ids)]
        pw.channel_iri_gen.nc.commit_list.side_effect = [result(chan_ids)]
        pw.ni.nc.commit_list.return_value = [{}] if reserve_response is None else reserve_response
        return pw

    def test_skips_ids_whose_channel_is_taken_and_reserves_both(self):
        pw = self.make_writer(['VFB_00100000'], ['VFBc_00100001'])
        ids = pw.reserve_image_ids('100000', 2, 'Clare72_curation:anat_x.tsv')
        self.assertEqual(['VFB_00100002', 'VFB_00100003'], ids)
        (statements,), _ = pw.ni.nc.commit_list.call_args
        self.assertEqual(['VFB_00100002', 'VFB_00100003', 'VFBc_00100002', 'VFBc_00100003'],
                         statements[0]['parameters']['ids'])
        self.assertIn('MERGE (r:IdReservation', statements[0]['statement'])

    def test_reserved_ids_are_not_minted_again_in_this_run(self):
        pw = self.make_writer([], [])
        first = pw.reserve_image_ids('100000', 1, 'r')
        self.assertNotEqual(first[0], pw.anat_iri_gen.generate('100000')['short_form'])

    def test_failed_reservation_raises(self):
        pw = self.make_writer([], [], reserve_response=False)
        with self.assertRaises(RuntimeError):
            pw.reserve_image_ids('100000', 1, 'r')

    def test_release_removes_anat_and_channel(self):
        pw = self.make_writer([], [])
        self.assertTrue(pw.release_ids(['VFB_00100002']))
        (statements,), _ = pw.ni.nc.commit_list.call_args
        self.assertEqual(['VFB_00100002', 'VFBc_00100002'], statements[0]['parameters']['ids'])


if __name__ == '__main__':
    unittest.main()
