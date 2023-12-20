#!/usr/bin/python3
# -*- coding: utf-8 -*-

########################################################################################################################
# Created by Jack Hangen
# Version 1
# Transform OOC dataset
########################################################################################################################

# Dependencies
import pandas as pd
import logging
from database_conn.db_conn import DataBaseConnector


def ooc_data_transform() -> pd.DataFrame:
    db_conn = DataBaseConnector()

    query = """
        SELECT
            RIGHT(Geography, 4) AS FIPS,
            EST_T_CE_POP_16_YO,
            EST_T_PERCENT_ALLOCATED_Occupation
        
        FROM occ;
        ;
    """

    df = pd.read_sql(query, db_conn.get_engine())
    logging.info("was able to read in ooc data")

    return df


if __name__ == "__main__":
    df = ooc_data_transform()

    print(df.head())
