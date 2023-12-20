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


def econ_data_transform() -> pd.DataFrame:
    db_conn = DataBaseConnector()

    query = """
    SELECT
    fips,
    (Pop_25_HS/Pop_25_EDUATT)*100 AS per_hs,
    ((Pop_25_SC + Pop_25_AD + Pop_25_COLL)/Pop_25_EDUATT)*100 AS per_coll,
    (Pop_25_GRAD/Pop_25_EDUATT)*100 AS per_grad
    
    FROM edu_att_test;
    """

    df = pd.read_sql(query, db_conn.get_engine())
    logging.info("was able to read in econ data")

    return df


if __name__ == "__main__":
    df = econ_data_transform()

    print(df.head())
