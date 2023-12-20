#!/usr/bin/python3
# -*- coding: utf-8 -*-

########################################################################################################################
# Created by Jack Hangen
# Version 1
# Collect Data
########################################################################################################################

# Dependencies
from Collect.Collect import CensusData, CollectElectionAPI, CreateFromCSV
from database_conn.db_conn import DataBaseConnector
from typing import Any
import logging


def import_csv_to_database(engine: Any, filename: str) -> None:
    """
    Imports data from a CSV file into a database table.

    This function reads data from a CSV file specified by the filename,
    creates a DataFrame from it, and then pushes the DataFrame to a SQL
    database table using the provided SQLAlchemy engine.

    Args:
        engine (Any): SQLAlchemy engine to be used for database operations.
        filename (str): Name of the CSV file (without the '.csv' extension) to be read.

    Raises:
        Exception: If any error occurs during file reading or database operations.
    """
    try:
        create_from_csv = CreateFromCSV(f'{filename}.csv', engine)
        create_from_csv.push_to_server(filename)
    except Exception as e:
        logging.error(f"Error importing CSV to database: {e}")
        raise


def main():
    db_conn = DataBaseConnector()
    engine = db_conn.get_engine()

    # push local files to server
    import_csv_to_database(engine, "FIPS")
    import_csv_to_database(engine, "edu_att_test")

    # push election API info to server
    election_api = CollectElectionAPI(engine)
    election_api.extract()
    election_api.push_to_server('elections')

    # push census data to server

    # age and sex data
    census_data = CensusData('AgeSexData.csv', engine)

    census_data.get_column_mappings('AgeSexData_columnMappings.csv')
    census_data.apply_column_mappings()
    census_data.convert_to_type_numeric()

    census_data.push_to_server('AgeSexData')

    # demographic and housing data
    census_data = CensusData('demographic_and_housing.csv', engine)

    census_data.get_column_mappings('demographic_and_housing_columnMappings.csv')
    census_data.apply_column_mappings()
    census_data.convert_to_type_numeric()
    census_data.drop_duplicate_columns()

    census_data.push_to_server('demographic_and_housing')

    # occupation and class of worker
    census_data = CensusData('occ.csv', engine)

    census_data.get_column_mappings('occ_columnMappings.csv')
    census_data.apply_column_mappings()
    census_data.convert_to_type_numeric()
    census_data.drop_duplicate_columns()

    census_data.push_to_server('occ')

    # Income
    census_data = CensusData('income.csv', engine)

    census_data.get_column_mappings('income_columnMappings.csv')
    census_data.apply_column_mappings()
    census_data.convert_to_type_numeric()
    census_data.drop_duplicate_columns()

    census_data.push_to_server('income')


if __name__ == "__main__":
    main()
