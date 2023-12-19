import unittest
from unittest.mock import patch, MagicMock, mock_open
from io import StringIO
import pandas as pd
from Collect.Collect import CollectElectionAPI


class TestCollectData(unittest.TestCase):

    def setUp(self):
        self.url = 'https://datawrapper.dwcdn.net/UI9i0/2/dataset.csv'
        self.engine = MagicMock()  # Mocking the SQLAlchemy engine
        self.collect_data = CollectElectionAPI(self.engine, self.url)

    @patch('requests.get')
    def test_extract_success(self, mock_get):
        # Simulate a successful response from the URL
        mock_get.return_value.ok = True
        mock_get.return_value.text = 'col1,col2\nval1,val2'
        self.collect_data.extract()
        self.assertIsNotNone(self.collect_data.data)

    @patch('requests.get')
    def test_extract_failure(self, mock_get):
        # Simulate a failed response from the URL
        mock_get.side_effect = Exception("Failed to fetch data")
        with self.assertRaises(Exception):
            self.collect_data.extract()

    @patch('pandas.read_csv')
    def test_get_df(self, mock_read_csv):
        # Mock the pandas read_csv method
        self.collect_data.data = StringIO('col1,col2\nval1,val2')
        mock_read_csv.return_value = pd.DataFrame({'col1': ['val1'], 'col2': ['val2']})
        df = self.collect_data.get_df()
        self.assertIsInstance(df, pd.DataFrame)

    @patch('pandas.DataFrame.to_sql')
    def test_push_to_server(self, mock_to_sql):
        # Mock the DataFrame to_sql method
        self.collect_data.data = StringIO('col1,col2\nval1,val2')
        self.collect_data.push_to_server('test_db')
        mock_to_sql.assert_called_once()


if __name__ == '__main__':
    unittest.main()
