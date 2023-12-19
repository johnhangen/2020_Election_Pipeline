#!/usr/bin/python3
# -*- coding: utf-8 -*-

########################################################################################################################
# Created by Jack Hangen
# Version 1
# Collect Election Data
########################################################################################################################

# Dependencies
from database_conn.db_conn import DataBaseConnector
from DNU.collect_data import CollectData


def main():

    db_conn = DataBaseConnector()

    cur = db_conn.get_engine()

    ext = CollectData(cur)
    ext.extract()
    ext.create_table()


if __name__ == "__main__":
    main()
