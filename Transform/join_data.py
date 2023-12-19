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


def join_data() -> pd.DataFrame:
    election_df = election_data_transform()
    fips_df = fips_data_transform()

    logging.info(f"shape of election data before {election_df.shape}")
    logging.info(f"shape of fips data before {fips_df.shape}")

    df_fips_election = election_df.merge(fips_df, how='left', left_on='FIPS', right_on='fips')

    econ_df = econ_data_transform()

    logging.info(f"shape of df after join {df_fips_election.shape}")
    logging.info(f"shape of econ df before join {econ_df.shape}")

    df_fips_election_econ = df_fips_election.merge(econ_df, how='left', left_on='FIPS', right_on='fips')

    dem_house_df = dem_house_data_transform()
    dem_house_df['FIPS'] = dem_house_df['FIPS'].astype(int)

    logging.info(f"shape of df after join {df_fips_election_econ.shape}")

    df_fips_election_econ_house = df_fips_election.merge(dem_house_df, how='left', left_on='FIPS', right_on='FIPS')

    age_sex_df = sex_age_data_transform()
    age_sex_df['FIPS'] = age_sex_df['FIPS'].astype(int)

    logging.info(f"shape of df after join {df_fips_election_econ.shape}")

    df_fips_election_econ_house_age = df_fips_election_econ_house.merge(age_sex_df, how='left', left_on='FIPS', right_on='FIPS')

    logging.info(f"shape of df after join {df_fips_election_econ.shape}")

    return df_fips_election_econ_house_age


def main():
    df = join_data()

    print(df.head())


if __name__ == "__main__":
    main()

