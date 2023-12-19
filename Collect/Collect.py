#!/usr/bin/python3
# -*- coding: utf-8 -*-

########################################################################################################################
# Created by Jack Hangen
# Version 1
# Collect Data
########################################################################################################################

# Dependencies
import logging
import os
from typing import Any, Optional, Dict
import requests
from io import StringIO
import pandas as pd
from abc import ABC, abstractmethod
from os.path import join, dirname, exists, isabs
from database_conn.db_conn import DataBaseConnector
from sqlalchemy.exc import SQLAlchemyError


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
innodb_strict_mode = 0


class PushDF(ABC):

    def __init__(self, engine):
        self.engine = engine

    @abstractmethod
    def push_to_server(self, db_name: str):
        pass

    @abstractmethod
    def read_in_df(self, filename: str):
        pass


class CensusData(PushDF):
    """
    CensusData class for processing and pushing census data to a database.

    Attributes:
        censusDF (pd.DataFrame): DataFrame containing census data.
        _column_mappings (Optional[Dict[str, str]]): Dictionary for column name mappings.
        engine (Any): Database engine for pushing data.
    """

    def __init__(self, census_df_filename: str, engine: Any):
        """
        Initialize CensusData with filename and database engine.

        Args:
            census_df_filename (str): Filename of the census data file.
            engine (Any): Database engine for pushing data.

        Raises:
            FileNotFoundError: If the specified file is not found.
        """
        super().__init__(engine)
        self._column_mappings = None
        self.censusDF = self.read_in_df(census_df_filename)

    def read_in_df(self, filename: str, file_type: str = 'csv', **kwargs) -> pd.DataFrame:
        """
        Read data into a DataFrame from a file.

        Args:
            filename (str): The name of the file to read.
            file_type (str): The type of the file (default is 'csv').

        Returns:
            pd.DataFrame: The loaded DataFrame.

        Raises:
            FileNotFoundError: If the file is not found.
            ValueError: If the file type is not supported.
            Exception: For other unexpected errors.
        """
        path = join(dirname(dirname(__file__)), f'data/{filename}')

        if not exists(path):
            logging.error(f"File not found: {path}")
            raise FileNotFoundError(f"File not found: {path}")

        try:
            if file_type == 'csv':
                return pd.read_csv(path, **kwargs)
            elif file_type == 'excel':
                return pd.read_excel(path, **kwargs)
            elif file_type == 'json':
                return pd.read_json(path, **kwargs)
            else:
                logging.error(f"Unsupported file type: {file_type}")
                raise ValueError(f"Unsupported file type: {file_type}")
        except Exception as e:
            logging.error(f"Error reading file: {e}")
            raise

    def get_column_mappings(self, column_mappings_file: str) -> None:
        """
        Retrieve column mappings from a file and set to the class attribute.

        Args:
            column_mappings_file (str): Filename of the column mappings file.

        Raises:
            FileNotFoundError: If the specified file is not found.
        """
        df_column_mappings = self.read_in_df(column_mappings_file)

        df_column_mappings['Label'] = df_column_mappings['Label'].apply(
            func=self.format_column_names
        )

        self._column_mappings = df_column_mappings.set_index('Column Name')['Label'].to_dict()

    @staticmethod
    def format_column_names(column_label: str) -> str:
        """
        Format column names based on predefined replacements.

        Args:
            column_label (str): The original column label.

        Returns:
            str: The formatted column label.
        """
        replacements = [
            ("!!", "_"),
            (" ", "_"),
            ("Total_", "T_"),
            ("Estimate", "EST"),
            ("Margin_of_Error_population", "MOE_POP"),
            ("Estimate_Percent", "EPER"),
            ("Estimate_population", "EPOP"),
            ("Margin_of_Error_Percent_population", "MOE_PER"),
            ("SUMMARY_INDICATORS", "SUM"),
            ("Margin_of_Error", "MOE"),
            ("years_and_over", "YO"),
            ("SELECTED_AGE_CATEGORIES", "YO"),
            ("population", "POP"),
            ("American_Indian_and_Alaska_Native", "AI"),
            ("Native_Hawaiian_and_Other_Pacific_Islander", "PI"),
            ("Two_or_more_races", "TWO+"),
            ("Black_or_African_American", "AA"),
            ("Race_alone_or_in_combination_with_one_or_more_other_races", "RACE_ALONE_POS"),
            ("Two_races_including_Some_other_race", "INC_OTH"),
            ("Two_races_excluding_Some_other_race", "EXC_OTH"),
            ("Hispanic_or_Latino", "HIS"),
            ("HISPANIC_OR_LATINO", "HIS"),
            ("and_Three_or_more_races", "3+"),
            ("VOTING_AGE_POPULATION_Citizen", "VOTE"),
            ("males_per_100_females", "MP100F")
        ]

        for old, new in replacements:
            column_label = column_label.replace(old, new)

        return column_label

    def apply_column_mappings(self) -> None:
        """
        Apply the column mappings to the DataFrame.
        """
        if self._column_mappings:
            self.censusDF.rename(columns=self._column_mappings, inplace=True)
        else:
            logging.warning("Column mappings are not set.")

        self.censusDF.drop(index=0, inplace=True)

    def check_for_duplicate_columns(self) -> None:
        """
        Checks for duplicate column names in the DataFrame.

        Raises:
            ValueError: If duplicate column names are found.
        """
        if self.censusDF.columns.duplicated().any():
            duplicate_columns = self.censusDF.columns[self.censusDF.columns.duplicated()].tolist()
            error_message = f"Duplicate column names found: {duplicate_columns}"
            logging.error(error_message)
            raise ValueError(error_message)

        logging.info("No duplicate columns found.")

    def drop_duplicate_columns(self) -> None:
        """
        Drops duplicate columns in the DataFrame, keeping the first occurrence.

        This method identifies and removes columns that have the same name.
        """
        # Creating a boolean series to identify duplicate columns
        is_duplicate = self.censusDF.columns.duplicated()

        if is_duplicate.any():
            # Dropping the duplicate columns
            self.censusDF = self.censusDF.loc[:, ~is_duplicate]
            logging.info("Duplicate columns dropped.")
        else:
            logging.info("No duplicate columns to drop.")

    def convert_to_type_numeric(self) -> None:
        try:
            exclude_columns = ['Geography', 'Geographic_Area_Name']
            columns_to_convert = self.censusDF.select_dtypes(exclude=['number']).columns.difference(exclude_columns)

            if not isinstance(columns_to_convert, list):
                columns_to_convert = list(columns_to_convert)

            for column in columns_to_convert:
                if isinstance(self.censusDF[column], pd.Series):
                    self.censusDF[column] = pd.to_numeric(self.censusDF[column], errors='coerce')
                else:
                    logging.warning(f"Column {column} is not a Series and was skipped.")

            logging.info("Successfully converted columns to numeric types.")
        except Exception as e:
            logging.error(f"Error during conversion to numeric types: {e}")
            raise

    def push_to_server(self, db_name: str, **sql_options) -> None:
        """
        Push the DataFrame to the specified database.

        Args:
            db_name (str): Name of the database to push data to.
            **sql_options: Additional SQL options for data pushing.

        Raises:
            SQLAlchemyError: If a database related error occurs.
            Exception: For other unexpected errors.
        """
        self.check_for_duplicate_columns()

        try:
            self.censusDF.to_sql(db_name, con=self.engine, if_exists='replace', index=False, **sql_options)
            logging.info(f"DataFrame successfully pushed to {db_name}.")
        except SQLAlchemyError as e:
            logging.error(f"Database error occurred: {e}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            raise


class CreateFromCSV(PushDF):
    """
    A class to create a SQL table from a CSV file using SQLAlchemy.

    Attributes:
        df (pd.DataFrame): DataFrame containing data from the CSV file.
        engine: SQLAlchemy engine for database operations.
    """

    def __init__(self, filename: str, engine: Any) -> None:
        """
        Initializes the CreateFromCSV object with the CSV file and SQLAlchemy engine.
        """
        super().__init__(engine)
        self.df = self.read_in_df(filename)

    def read_in_df(self, filename: str, file_type: str = 'csv', **kwargs) -> pd.DataFrame:
        """
        Read data into a DataFrame from a file.

        Args:
            filename (str): The name of the file to read.
            file_type (str): The type of the file (default is 'csv').

        Returns:
            pd.DataFrame: The loaded DataFrame.

        Raises:
            FileNotFoundError: If the file is not found.
            ValueError: If the file type is not supported.
            Exception: For other unexpected errors.
        """
        # Flexible file path handling
        path = filename if isabs(filename) else join(dirname(dirname(__file__)), f'data/{filename}')

        if not os.path.exists(path):
            logging.error(f"File not found: {path}")
            raise FileNotFoundError(f"File not found: {path}")

        try:
            if file_type == 'csv':
                return pd.read_csv(path, **kwargs)
            elif file_type == 'excel':
                return pd.read_excel(path, **kwargs)
            elif file_type == 'json':
                return pd.read_json(path, **kwargs)
            else:
                logging.error(f"Unsupported file type: {file_type}")
                raise ValueError(f"Unsupported file type: {file_type}")
        except Exception as e:
            logging.error(f"Error reading file: {e}")
            raise

    def push_to_server(self, db_name: str, **sql_options) -> None:
        """
        Push the DataFrame to the specified database.

        Args:
            db_name (str): Name of the database to push data to.
            **sql_options: Additional SQL options for data pushing.

        Raises:
            SQLAlchemyError: If a database related error occurs.
            Exception: For other unexpected errors.
        """

        try:
            self.df.to_sql(db_name, con=self.engine, if_exists='replace', index=False, **sql_options)
            logging.info(f"DataFrame successfully pushed to {db_name}.")
        except SQLAlchemyError as e:
            logging.error(f"Database error occurred: {e}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            raise


class CollectElectionAPI(PushDF, ABC):
    """
    Class to extract data from a specified URL and load it into a database.

    Attributes:
        data (StringIO): Buffer to hold downloaded data.
        url (str): URL to fetch data from.
        engine: SQLAlchemy engine for database operations.
    """

    def __init__(self, engine, url: str = 'https://datawrapper.dwcdn.net/UI9i0/2/dataset.csv'):
        """
        Initializes ExtractData with a SQLAlchemy engine and a data URL.
        """
        super().__init__(engine)
        self.data = None
        self.url = url

    def read_in_df(self, filename: str, file_type: str = 'csv', **kwargs) -> pd.DataFrame:
        """
        Read data into a DataFrame from a file.

        Args:
            filename (str): The name of the file to read.
            file_type (str): The type of the file (default is 'csv').

        Returns:
            pd.DataFrame: The loaded DataFrame.

        Raises:
            FileNotFoundError: If the file is not found.
            ValueError: If the file type is not supported.
            Exception: For other unexpected errors.
        """
        # Flexible file path handling
        path = filename if isabs(filename) else join(dirname(dirname(__file__)), f'data/{filename}')

        if not os.path.exists(path):
            logging.error(f"File not found: {path}")
            raise FileNotFoundError(f"File not found: {path}")

        try:
            if file_type == 'csv':
                return pd.read_csv(path, **kwargs)
            elif file_type == 'excel':
                return pd.read_excel(path, **kwargs)
            elif file_type == 'json':
                return pd.read_json(path, **kwargs)
            else:
                logging.error(f"Unsupported file type: {file_type}")
                raise ValueError(f"Unsupported file type: {file_type}")
        except Exception as e:
            logging.error(f"Error reading file: {e}")
            raise

    def extract(self) -> None:
        """
        Extracts data from the specified URL and stores it in 'data'.
        """
        try:
            response = requests.get(self.url)
            response.raise_for_status()  # Raise an HTTPError for bad responses
            self.data = StringIO(response.text)
            logging.info("Data successfully extracted from URL.")
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error occurred while extracting data: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logging.error(f"Error occurred while making request: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error occurred while extracting data: {e}")
            raise

    def get_df(self) -> pd.DataFrame:
        """
        Converts the extracted data into a Pandas DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing the extracted data.
        """
        try:
            df = pd.read_csv(self.data)
            return df
        except pd.errors.ParserError as e:
            logging.error(f"Error parsing CSV data: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error occurred while converting data to DataFrame: {e}")
            raise

    def push_to_server(self, db_name: str, **sql_options) -> None:
        """
        Push the DataFrame to the specified database.
        """
        df = self.get_df()

        try:
            df.to_sql(db_name, con=self.engine, if_exists='replace', index=False, **sql_options)
            logging.info(f"DataFrame successfully pushed to {db_name}.")
        except SQLAlchemyError as e:
            logging.error(f"Database error occurred: {e}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            raise


def temp():
    db_conn = DataBaseConnector()

    census_data = CensusData('AgeSexData.csv', db_conn.get_engine())

    census_data.get_column_mappings('AgeSexData_columnMappings.csv')
    census_data.apply_column_mappings()
    census_data.convert_to_type_numeric()

    census_data.push_to_server('AgeSexData')


def main():
    db_conn = DataBaseConnector()

    census_data = CensusData('demographic_and_housing.csv', db_conn.get_engine())

    census_data.get_column_mappings('demographic_and_housing_columnMappings.csv')
    census_data.apply_column_mappings()
    census_data.convert_to_type_numeric()
    census_data.drop_duplicate_columns()

    census_data.push_to_server('demographic_and_housing')


if __name__ == "__main__":
    main()
