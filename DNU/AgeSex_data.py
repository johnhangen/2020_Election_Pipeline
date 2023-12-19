#!/usr/bin/python3
# -*- coding: utf-8 -*-

########################################################################################################################
# Created by Jack Hangen
# Version 1
# Collect Data
########################################################################################################################

# Dependencies
import logging
import pandas as pd

from Collect.Collect import CensusData
from database_conn.db_conn import DataBaseConnector


def main():
    db_conn = DataBaseConnector()

    census_data = CensusData('AgeSexData.csv', db_conn.get_engine())


if __name__ == "__main__":
    main()
