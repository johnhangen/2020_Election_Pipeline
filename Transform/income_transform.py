#!/usr/bin/python3
# -*- coding: utf-8 -*-

########################################################################################################################
# Created by Jack Hangen
# Version 1
# Transform income dataset
########################################################################################################################

# Dependencies
import pandas as pd
import logging
from database_conn.db_conn import DataBaseConnector


def income_data_transform() -> pd.DataFrame:
    db_conn = DataBaseConnector()

    query = """
        SELECT
            RIGHT(Geography, 5) AS FIPS,
            `EST_HH_Median_income_(dollars)`,
            `MOE_HH_Median_income_(dollars)`,
            `EST_HH_Mean_income_(dollars)`,
            `MOE_HH_Mean_income_(dollars)`
        
        FROM income
        ;
    """

    df = pd.read_sql(query, db_conn.get_engine())
    logging.info("was able to read in income data")

    return df


if __name__ == "__main__":
    df = income_data_transform()
