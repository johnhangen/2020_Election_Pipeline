#!/usr/bin/python3
# -*- coding: utf-8 -*-

########################################################################################################################
# Created by Jack Hangen
# Version 1
# transform election data set
########################################################################################################################

# Dependencies
import pandas as pd
import logging
from database_conn.db_conn import DataBaseConnector


def election_data_transform() -> pd.DataFrame:
    db_conn = DataBaseConnector()

    query = """
    SELECT
    FIPS,
    Code,
    County,
    Population,
    2020W AS 2020_winner,
    2020D AS DEM_per,
    2020R AS REP_per,
    2020O AS OTH_per,
    2016W as 2016_winner
    
    FROM elections;
    """

    df = pd.read_sql(query, db_conn.get_engine())
    logging.info("was able to read in election data")

    df.replace({"2016_winner": {
        "Trump": "REP",
        "Clinton": "DEM"
    }}, inplace=True)

    return df


if __name__ == "__main__":
    election_data_transform()
