#!/usr/bin/python3
# -*- coding: utf-8 -*-

########################################################################################################################
# Created by Jack Hangen
# Version 1
# Collect Data
########################################################################################################################

# Dependencies
from DNU.collect_data import CreateFromCSV
from database_conn.db_conn import DataBaseConnector


def main():
    db_conn = DataBaseConnector()

    eng = db_conn.get_engine()

    CreateFromCSV(file='edu_att_test.csv', engine=eng).create_sql_table('edu_att_test')


if __name__ == "__main__":
    main()
