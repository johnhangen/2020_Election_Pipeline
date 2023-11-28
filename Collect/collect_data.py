#!/usr/bin/python3
# -*- coding: utf-8 -*-

########################################################################################################################
# Created by Jack Hangen
# Version 1
# Collect Data
########################################################################################################################

# Dependencies
import requests
import logging
import pandas as pd
from io import StringIO
from database_conn.db_conn import DataBaseConnector, load_env_vars
from os.path import join, dirname


class CollectData:
    """
    Class to extract data from a specified URL and load it into a database.

    Attributes:
        data (StringIO): Buffer to hold downloaded data.
        url (str): URL to fetch data from.
        cur: Database cursor or connection object for database operations.
    """

    def __init__(self, curr, url: str = 'https://datawrapper.dwcdn.net/UI9i0/2/dataset.csv'):
        """
        Initializes ExtractData with a database cursor and a data URL.

        Args:
            curr: Database cursor or connection object for executing database commands.
            url (str): URL to fetch data from.
        """
        self.data = None
        self.url = url
        self.cur = curr

    def extract(self) -> None:
        """
        Extracts data from the specified URL and stores it in 'data'.
        """
        try:
            response = requests.get(self.url)
            self.data = StringIO(response.text)
        except Exception as e:
            logging.error(f"Error extracting data from API {e}")
            raise Exception(f"Error extracting data from API {e}")
        finally:
            logging.info("was able to get data from api")

    def get_df(self) -> pd.DataFrame:
        """
        Converts the extracted data into a Pandas DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing the extracted data.
        """
        return pd.read_csv(self.data, sep=',')

    def create_table(self) -> None:
        """
        Creates a table in the database from the extracted DataFrame.
        """
        df = self.get_df()
        df.to_sql(name="elections", con=self.cur, if_exists='replace', index=False)
        logging.info("Table 'elections' created/updated in the database")


class CreateFromCSV:
    """
    A class to create a SQL table from a CSV file using SQLAlchemy.

    Attributes:
        file (str): The name of the CSV file to be read.
        path (str): The path to the directory where the CSV file is located.
        engine: The SQLAlchemy engine to be used for database operations.
    """

    def __init__(self, file: str, engine):
        """
        Initializes the CreateFromCSV object with the CSV file and SQLAlchemy engine.

        Args:
            file (str): The name of the CSV file to be read.
            engine: The SQLAlchemy engine to be used for database operations.
        """
        self.file = file
        self.path = join(dirname(dirname(__file__)), 'data')
        self.engine = engine

    def create_sql_table(self, db_name: str = 'fips') -> None:
        """
        Reads the CSV file and creates a SQL table from its contents.

        The method reads the CSV file into a pandas DataFrame and then
        uses the SQLAlchemy engine to create a SQL table with the contents
        of the DataFrame.

        Args:
            db_name (str): The name of the database table to be created or replaced.
                           Defaults to 'fips'.
        """
        try:
            df = pd.read_csv(join(self.path, self.file))
            df.to_sql(db_name, con=self.engine, if_exists='replace', index=False)
        except Exception as e:
            raise Exception(f"An error occurred while creating the SQL table: {e}")
