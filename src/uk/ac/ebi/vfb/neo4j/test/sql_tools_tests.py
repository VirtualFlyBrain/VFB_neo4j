import os
import unittest
from unittest import mock

import psycopg2

from uk.ac.ebi.vfb.neo4j import SQL_tools


class TestGetFbConn(unittest.TestCase):

    def test_default_is_flybase(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(['chado.flybase.org'], SQL_tools.fb_chado_hosts())

    def test_falls_back_in_order(self):
        calls = []

        def connect(**kw):
            calls.append(kw['host'])
            if kw['host'] == 'chado.pipeline.svc.cluster.local':
                raise psycopg2.OperationalError('database "flybase" does not exist')
            return 'conn'

        with mock.patch.dict(os.environ, {'FB_CHADO_HOST': 'chado.pipeline.svc.cluster.local, chado.flybase.org'}), \
                mock.patch.object(SQL_tools.psycopg2, 'connect', side_effect=connect):
            self.assertEqual('conn', SQL_tools.get_fb_conn())
        self.assertEqual(['chado.pipeline.svc.cluster.local', 'chado.flybase.org'], calls)

    def test_last_failure_raises(self):
        with mock.patch.dict(os.environ, {'FB_CHADO_HOST': 'a,b'}), \
                mock.patch.object(SQL_tools.psycopg2, 'connect', side_effect=psycopg2.OperationalError('no')):
            with self.assertRaises(psycopg2.OperationalError):
                SQL_tools.get_fb_conn()


if __name__ == '__main__':
    unittest.main()
