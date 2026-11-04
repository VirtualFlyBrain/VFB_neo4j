import unittest
from unittest import mock

from psycopg2 import OperationalError

from uk.ac.ebi.vfb.neo4j import SQL_tools


class TestGetFbConn(unittest.TestCase):
    """get_fb_conn retries a refused connection (chado.flybase.org max_client_conn) before giving up."""

    @mock.patch.object(SQL_tools.time, 'sleep')
    @mock.patch.object(SQL_tools.psycopg2, 'connect')
    def test_retries_then_connects(self, connect, sleep):
        conn = object()
        connect.side_effect = [OperationalError('no more connections allowed'), conn]
        self.assertIs(conn, SQL_tools.get_fb_conn(delay=1))
        self.assertEqual(2, connect.call_count)
        sleep.assert_called_once_with(1)

    @mock.patch.object(SQL_tools.time, 'sleep')
    @mock.patch.object(SQL_tools.psycopg2, 'connect')
    def test_raises_after_max_retries(self, connect, sleep):
        connect.side_effect = OperationalError('no more connections allowed')
        with self.assertRaises(OperationalError):
            SQL_tools.get_fb_conn(max_retries=3, delay=1)
        self.assertEqual(3, connect.call_count)
        self.assertEqual(2, sleep.call_count)


if __name__ == '__main__':
    unittest.main()
