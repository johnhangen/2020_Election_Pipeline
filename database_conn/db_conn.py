#!/usr/bin/python3
# -*- coding: utf-8 -*-

########################################################################################################################
# Created by Jack Hangen
# Version 1
# Collect Data
########################################################################################################################

# Dependencies
import logging
import pymysql
from pymysql.cursors import Cursor
from pymysql.connections import Connection
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from os.path import join, dirname


def load_env_vars() -> dict:
    """
    Loads environment variables from a .env file and validates their presence.

    This function first determines the path of the .env file located one level up from the current script's directory.
    It then loads environment variables using the dotenv module. After loading the variables, it checks for the presence
    of all required environment variables listed in 'required_vars'. If any of these variables are not set, the function
    logs an error and raises an EnvironmentError.

    Returns:
        dict: A dictionary containing all the required environment variables.

    Raises:
        EnvironmentError: If any of the required environment variables is not set.

    Note:
        The required environment variables are: ENDPOINT, PORT, USER, REGION, PASSWORD, DBNAME.
    """
    dotenv_path = join(dirname(dirname(__file__)), '.env')
    load_dotenv(dotenv_path)

    required_vars = ["ENDPOINT", "PORT", "USER", "REGION", "PASSWORD", "DBNAME"]
    env_vars_test = {}

    for var in required_vars:
        value = os.environ.get(var)
        if value is None:
            logging.error(f"Required environment variable '{var}' is not set.")
            raise EnvironmentError(f"Required environment variable '{var}' is not set.")
        env_vars_test[var] = value

    return env_vars_test


class DataBaseConnector:
    """
    A database connector class for establishing and managing a connection to an AWS RDS instance using PyMySQL.

    Attributes:
        conn (Connection): A PyMySQL connection object.
        cur (Cursor): A PyMySQL cursor object for executing database commands.
        endpoint (str): The database server's endpoint URL.
        port (int): The port number on which the database is listening.
        user (str): The username for the database.
        region (str): The region of the AWS RDS instance.
        password (str): The password associated with the database user.
        dbname (str): The name of the database to connect to.
    """

    def __init__(self):
        """
        Initializes the DataBaseConnector class.

        This constructor initializes the database connection parameters by loading them
        from environment variables. It sets up the necessary attributes for the database
        connection and attempts to retrieve the connection details (endpoint, port, user,
        region, password, and dbname) from the environment variables using the
        load_env_vars function.

        If any of the required environment variables are missing or if there is an error
        during the loading of these variables, an error message is logged, and the program
        exits.
        """
        self.conn = None
        self.cur = None

        try:
            env_vars = load_env_vars()
            self.endpoint = env_vars["ENDPOINT"]
            self.port = int(env_vars["PORT"])
            self.user = env_vars["USER"]
            self.region = env_vars["REGION"]
            self.password = env_vars["PASSWORD"]
            self.dbname = env_vars["DBNAME"]
        except EnvironmentError as e:
            logging.error(f"Error loading in env vars {e}")
            print(e)
            exit(1)

    def get_conn(self) -> Connection:
        """
         Establishes and returns a PyMySQL connection to the AWS RDS instance.

         This method attempts to establish a connection using the instance's
         credentials and configuration. If successful, it sets and returns
         the connection object. In case of failure, an error is logged.

         Returns:
             Connection: A PyMySQL connection object to the AWS RDS instance.
         """
        try:
            self.conn = pymysql.connect(
                host=self.endpoint,
                port=self.port,
                user=self.user,
                passwd=self.password,
                db=self.dbname)
            logging.info("Successful connection to AWS RDS")
        except pymysql.MySQLError as e:
            logging.error(f"Error connecting to AWS RDS: {e}")
        finally:
            return self.conn

    def get_engine(self):
        """
        Creates and returns a SQLAlchemy engine using the database connection details.

        This method uses the instance's connection details to create a SQLAlchemy engine,
        which can be used to interact with the database using SQLAlchemy's ORM features.

        Returns:
            Engine: A SQLAlchemy engine connected to the database.
        """
        # Construct the database URL
        database_url = f"mysql+pymysql://{self.user}:{self.password}@{self.endpoint}:{self.port}/{self.dbname}"
        # Create and return the SQLAlchemy engine
        return create_engine(database_url)

    def get_cur(self) -> Cursor:
        """
        Creates and returns a cursor object using the established database connection.

        This method attempts to create a cursor object for executing SQL commands.
        If the connection is not established, it tries to establish it first. In
        case of failure in creating a cursor, an error is logged.

        Returns:
            Cursor: A PyMySQL cursor object for the database connection.
        """
        try:
            self.cur = self.get_conn().cursor()
        except Exception as e:
            logging.error(f"Error creating cursor object: {e}")
        finally:
            return self.cur

    def close_conn(self) -> None:
        """
        Closes the database connection.

        This method safely closes the database connection if it's open.
        """
        if self.conn and self.conn.open:
            self.conn.close()
            logging.info("Database connection closed")

    def close_cur(self) -> None:
        """
        Closes the cursor object.

        This method safely closes the cursor object if it's open.
        """
        if self.cur:
            self.cur.close()
            logging.info("Cursor closed")

    def __enter__(self):
        """
        Enters the runtime context related to this object. The 'with' statement will bind this method's return value
        to the target specified in the 'as' clause of the statement, if any.

        Returns:
            self (DataBaseConnector): The current instance of DataBaseConnector.
        """
        self.get_conn()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exits the runtime context and closes the database connection and cursor. This method handles the closing
        of resources whether an exception occurred in the 'with' block.

        Args:
            exc_type: The type of the exception (if any).
            exc_val: The value of the exception (if any).
            exc_tb: The traceback of the exception (if any).
        """
        self.close_cur()
        self.close_conn()


if __name__ == "__main__":

    db_conn = DataBaseConnector()

    cur = db_conn.get_cur()

    cur.execute("""SELECT now();""")
    query_results = cur.fetchall()
    print(query_results)

    db_conn.close_conn()
    db_conn.close_cur()
