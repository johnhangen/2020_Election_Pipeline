#!/usr/bin/python3
# -*- coding: utf-8 -*-

########################################################################################################################
# Created by Jack Hangen
# Version 1
# Transform dem house dataset
########################################################################################################################

# Dependencies
import pandas as pd
import logging
from database_conn.db_conn import DataBaseConnector


def dem_house_data_transform() -> pd.DataFrame:
    db_conn = DataBaseConnector()

    query = """
    SELECT 
        RIGHT(Geography, 4) AS FIPS,
        SUBSTRING_INDEX(Geographic_Area_Name, ',', 1) AS COUNTY,
        SUBSTRING_INDEX(Geographic_Area_Name, ',', -1) AS STATE,
        EST_RACE_T_POP_One_race_White,
        EST_RACE_T_POP_One_race_AA,
        EST_RACE_T_POP_One_race_AI,
        EST_RACE_T_POP_One_race_Asian,
        Percent_T_housing_units,
        'Percent_CITIZEN,_VOTE,_18_and_over_POP',
        'Percent_CITIZEN,_VOTE,_18_and_over_POP_Male',
        'Percent_CITIZEN,_VOTE,_18_and_over_POP_Female'
    
    FROM demographic_and_housing;
    """

    df = pd.read_sql(query, db_conn.get_engine())
    logging.info("was able to read in dem and house data")

    return df


if __name__ == "__main__":
    df = dem_house_data_transform()

    print(df.head())