#!/usr/bin/python3
# -*- coding: utf-8 -*-

########################################################################################################################
# Created by Jack Hangen
# Version 1
# testing for the census data class
########################################################################################################################

# Dependencies
import unittest
from unittest.mock import Mock, patch, mock_open
import pandas as pd
from Collect.Collect import CensusData


class TestCensusData(unittest.TestCase):

    def setUp(self):
        self.mock_df = pd.DataFrame({'Column1': [1, 2], 'Column2': [3, 4]})
        self.mock_engine = Mock()
        self.census_data = CensusData('test_file.csv', self.mock_engine)

    def test_init(self):
        self.assertIsNotNone(self.census_data)

    @patch('pandas.read_csv')
    def test_read_in_df_csv(self, mock_read_csv):
        mock_read_csv.return_value = self.mock_df
        df = self.census_data.read_in_df('test_file.csv')
        self.assertEqual(df.shape, self.mock_df.shape)

    @patch('builtins.open', new_callable=mock_open)
    @patch('pandas.read_csv')
    def test_get_column_mappings(self, mock_read_csv, mock_file):
        mock_read_csv.return_value = pd.DataFrame({
            'Column Name': ['col1', 'col2'],
            'Label': ['label1', 'label2']
        })
        self.census_data.get_column_mappings('test_column_mappings.csv')
        self.assertEqual(self.census_data._column_mappings, {'col1': 'label1', 'col2': 'label2'})

    def test_format_column_names(self):
        formatted_name = self.census_data.format_column_names("!!Total_Margin_of_Error_population")
        self.assertEqual(formatted_name, "_MOE_POP")

    def test_apply_column_mappings(self):
        self.census_data._column_mappings = {'Column1': 'NewColumn1', 'Column2': 'NewColumn2'}
        self.census_data.censusDF = self.mock_df
        self.census_data.apply_column_mappings()
        self.assertIn('NewColumn1', self.census_data.censusDF.columns)

    def test_convert_to_type_numeric(self):
        mock_df = pd.DataFrame({'A': ['1', '2'], 'B': ['3', '4'], 'Geography': ['X', 'Y']})
        self.census_data.censusDF = mock_df
        self.census_data.convert_to_type_numeric()
        self.assertTrue(pd.api.types.is_numeric_dtype(self.census_data.censusDF['A']))
        self.assertTrue(pd.api.types.is_numeric_dtype(self.census_data.censusDF['B']))

    @patch('pandas.DataFrame.to_sql')
    def test_push_to_server(self, mock_to_sql):
        self.census_data.censusDF = self.mock_df
        self.census_data.push_to_server('test_db')
        mock_to_sql.assert_called_once()

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

