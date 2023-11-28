#!/usr/bin/python3
# -*- coding: utf-8 -*-

########################################################################################################################
# Created by Jack Hangen
# Version 1
# Transform fips dataset
########################################################################################################################

# Dependencies
import pandas as pd
import logging
from database_conn.db_conn import DataBaseConnector


def fips_data_transform() -> pd.DataFrame:
    db_conn = DataBaseConnector()

    query = """
    SELECT 
    fips,
    county,
    state_abbr,
    state
    FROM fips;
    """

    df = pd.read_sql(query, db_conn.get_engine())
    logging.info("was able to read in fips data")

    return df


if __name__ == "__main__":
    df = fips_data_transform()

    print(df.head())
