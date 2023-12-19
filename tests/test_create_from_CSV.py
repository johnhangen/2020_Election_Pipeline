import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from Collect.Collect import CreateFromCSV


class TestCreateFromCSV(unittest.TestCase):

    def setUp(self):
        self.filename = 'test_file.csv'
        self.engine = MagicMock()  # Mocking the SQLAlchemy engine
        self.create_from_csv = CreateFromCSV(self.filename, self.engine)

    @patch('pandas.read_csv')
    def test_read_in_df_csv(self, mock_read_csv):
        # Simulate reading a CSV file
        mock_df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        mock_read_csv.return_value = mock_df
        df = self.create_from_csv.read_in_df(self.filename)
        mock_read_csv.assert_called_once()
        self.assertEqual(df.shape, mock_df.shape)

    @patch('os.path.exists')
    @patch('pandas.read_csv')
    def test_read_in_df_file_not_found(self, mock_read_csv, mock_exists):
        # Simulate file not found
        mock_exists.return_value = False
        with self.assertRaises(FileNotFoundError):
            self.create_from_csv.read_in_df('nonexistent.csv')

    @patch('pandas.DataFrame.to_sql')
    def test_push_to_server(self, mock_to_sql):
        # Mock the DataFrame to_sql method
        mock_df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        self.create_from_csv.df = mock_df
        db_name = 'test_db'
        self.create_from_csv.push_to_server(db_name)
        mock_to_sql.assert_called_once()


if __name__ == '__main__':
    unittest.main()
