#!/usr/bin/python3
# -*- coding: utf-8 -*-
import logging

########################################################################################################################
# Created by Jack Hangen
# Version 1
# Complete data pipline
########################################################################################################################

# Dependencies
from Transform.join_data import join_data
from database_conn.db_conn import DataBaseConnector
from os.path import join, dirname


def main():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s',
                        filename=join(dirname(dirname(__file__)), 'logging/pol_pipeline.log'),
                        filemode='w')

    db_conn = DataBaseConnector()

    df = join_data()

    try:
        df.to_sql("POL_FINAL", con=db_conn.get_engine(), if_exists='replace', index=False)
    except Exception as e:
        raise Exception(f"An error occurred while creating the SQL table: {e}")


if __name__ == "__main__":
    main()
