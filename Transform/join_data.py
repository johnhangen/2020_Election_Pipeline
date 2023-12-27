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
from Transform.election_transform import election_data_transform
from Transform.fip_transform import fips_data_transform
from Transform.econ_transform import econ_data_transform
from Transform.dem_housing_transform import dem_house_data_transform
from Transform.AgeSexData_transform import sex_age_data_transform
from Transform.income_transform import income_data_transform
from Transform.ooc import ooc_data_transform


def join_data() -> pd.DataFrame:
    election_df = election_data_transform()
    fips_df = fips_data_transform()

    logging.info(f"shape of election data before {election_df.shape}")
    logging.info(f"shape of fips data before {fips_df.shape}")

    df_fips_election = election_df.merge(fips_df, how='left', left_on='FIPS', right_on='fips')

    # econ
    econ_df = econ_data_transform()

    df_fips_election_econ = df_fips_election.merge(econ_df, how='left', left_on='FIPS', right_on='fips')

    logging.info(f"ECON: {df_fips_election.shape}")

    #dem and housing
    dem_house_df = dem_house_data_transform()
    dem_house_df['FIPS'] = dem_house_df['FIPS'].astype(int)

    df_fips_election_econ_house = df_fips_election_econ.merge(dem_house_df, how='left', left_on='FIPS', right_on='FIPS')

    logging.info(f"DEM AND HOUSING {df_fips_election_econ_house.shape}")

    # age sex
    age_sex_df = sex_age_data_transform()
    age_sex_df['FIPS'] = age_sex_df['FIPS'].astype(int)

    df_fips_election_econ_house_age = df_fips_election_econ_house.merge(age_sex_df, how='left', left_on='FIPS', right_on='FIPS')

    logging.info(f"AGE SEX {df_fips_election_econ_house_age.shape}")

    # income
    inc_df = income_data_transform()
    inc_df['FIPS'] = inc_df['FIPS'].astype(int)

    df_fips_election_econ_house_age_inc = df_fips_election_econ_house_age.merge(inc_df, how='left', left_on='FIPS', right_on='FIPS')

    logging.info(f"INCOME {df_fips_election_econ_house_age_inc.shape}")

    #OOO
    ooc_df = ooc_data_transform()
    ooc_df['FIPS'] = ooc_df['FIPS'].astype(int)

    df_fips_election_econ_house_age_inc_ooc = df_fips_election_econ_house_age_inc.merge(ooc_df, how='left', left_on='FIPS', right_on='FIPS')

    logging.info(f"OOO {df_fips_election_econ_house_age_inc_ooc.shape}")

    return df_fips_election_econ_house_age_inc_ooc


def main():
    df = join_data()

    print(df.head())

    df.to_csv(r'C:\Users\jthan\PycharmProjects\Election_pipline\output\final.csv')


if __name__ == "__main__":
    main()

