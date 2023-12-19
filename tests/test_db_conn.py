import unittest
from unittest.mock import patch, MagicMock
from database_conn.db_conn import DataBaseConnector


class TestDataBaseConnector(unittest.TestCase):

    def setUp(self):
        # Setup for each test
        self.db_connector = DataBaseConnector()

    @patch('database_conn.db_conn.pymysql.connect')
    def test_get_conn(self, mock_connect):
        # Test successful database connection
        mock_connect.return_value = MagicMock()
        conn = self.db_connector.get_conn()
        mock_connect.assert_called_once()
        self.assertIsNotNone(conn)

    @patch('database_conn.db_conn.pymysql.connect')
    def test_get_conn_failure(self, mock_connect):
        # Test database connection failure
        mock_connect.side_effect = Exception("Connection failed")
        with self.assertRaises(Exception):
            self.db_connector.get_conn()

    @patch('database_conn.db_conn.create_engine')
    def test_get_engine(self, mock_create_engine):
        # Test creating SQLAlchemy engine
        mock_create_engine.return_value = MagicMock()
        engine = self.db_connector.get_engine()
        mock_create_engine.assert_called_once()
        self.assertIsNotNone(engine)

    @patch('database_conn.db_conn.pymysql.connect')
    def test_connection_context_manager(self, mock_connect):
        # Test the context manager functionality
        mock_connect.return_value = MagicMock()
        with self.db_connector.connection() as connector:
            self.assertIsNotNone(connector)
            self.assertTrue(hasattr(connector, 'get_cur'))
        mock_connect.assert_called_once()

    def tearDown(self):
        # Cleanup after each test
        pass


if __name__ == '__main__':
    unittest.main()
