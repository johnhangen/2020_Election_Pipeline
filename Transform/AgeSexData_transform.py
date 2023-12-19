#!/usr/bin/python3
# -*- coding: utf-8 -*-

########################################################################################################################
# Created by Jack Hangen
# Version 1
# Transform AgeSex dataset
########################################################################################################################

# Dependencies
import pandas as pd
import logging
from database_conn.db_conn import DataBaseConnector


def sex_age_data_transform() -> pd.DataFrame:
    db_conn = DataBaseConnector()

    query = """
    SELECT 
        RIGHT(Geography, 4) AS FIPS,
        SUBSTRING_INDEX(Geographic_Area_Name, ',', 1) AS COUNTY,
        SUBSTRING_INDEX(Geographic_Area_Name, ',', -1) AS STATE,
        EST_Percent_T_POP_AGE_20_to_24_years,
        EST_Percent_T_POP_AGE_25_to_29_years,
        EST_Percent_T_POP_AGE_30_to_34_years
        EST_Percent_T_POP_AGE_35_to_39_years,
        EST_Percent_T_POP_AGE_40_to_44_years,
        EST_Percent_T_POP_AGE_45_to_49_years,
        EST_Percent_T_POP_AGE_50_to_54_years,
        EST_Percent_T_POP_AGE_55_to_59_years,
        EST_Percent_T_POP_AGE_60_to_64_years,
        EST_Percent_T_POP_AGE_65_to_69_years,
        EST_Percent_T_POP_AGE_70_to_74_years,
        EST_Percent_T_POP_AGE_75_to_79_years,
        EST_Percent_T_POP_AGE_80_to_84_years,
        EST_Percent_T_POP_AGE_85_YO,
        'EST_Percent_Female_T_POP_SUM_Sex_ratio_(MP100F)'
    
    FROM AgeSexData;
    """

    df = pd.read_sql(query, db_conn.get_engine())
    logging.info("was able to read in sex and age data")

    return df


if __name__ == "__main__":
    df = sex_age_data_transform()

    print(df.head())
