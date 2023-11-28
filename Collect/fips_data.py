#!/usr/bin/python3
# -*- coding: utf-8 -*-

########################################################################################################################
# Created by Jack Hangen
# Version 1
# Collect Data
########################################################################################################################

# Dependencies
import logging
from Collect.collect_data import CreateFromCSV
from database_conn.db_conn import DataBaseConnector, load_env_vars


def main():

    db_conn = DataBaseConnector()

    eng = db_conn.get_engine()

    CreateFromCSV(file='FIPS.csv', engine=eng).create_sql_table('fips')


if __name__ == "__main__":
    main()
