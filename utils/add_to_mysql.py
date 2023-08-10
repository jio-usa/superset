# https://medium.com/@vedashri.debray/setting-up-apache-superset-on-dockers-and-connecting-it-to-a-mysql-docker-container-a-foolproof-37fe5936ae83

# This file contains the script for adding the data to the MySQL database

import pandas as pd
import os
import pymysql
from sqlalchemy import create_engine
from tqdm import tqdm
from logging import getLogger

logger = getLogger(__name__)

DATA_DIR = '/home/labuser/Desktop/anomaly-detection/data/raw/backup/'
ercs = create_engine('mysql+pymysql://root:atoc@localhost:3306/network_kpis_ercs')
nokia = create_engine('mysql+pymysql://root:atoc@localhost:3306/network_kpis_nokia')

kpis = pd.read_excel('kpi_list.xlsx')
kpis = kpis['KPINAME'].tolist()

def read_and_preprocess(file):
    """
    Preprocess the data to only use the required columns
    """
    df = pd.read_csv(os.path.join(DATA_DIR, file))
    k = [x for x in kpis if x in df.columns]
    df = df[k]
    return df

def add_to_sql(file, table_name, engine):
    """
    Add the data to the MySQL database
    """
    df = read_and_preprocess(file)
    df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=1000)
    df = pd.DataFrame()

def main():
    for file in tqdm(os.listdir(DATA_DIR)):
        if file.endswith(".csv"):
            if file.split('_')[2] == 'ERCS':
                if file.split('_')[4] == 'BBH':
                    add_to_sql(file, 'BBH', ercs)
                elif file.split('_')[4] == 'NBH':
                    add_to_sql(file, 'NBH', ercs)
                else:
                    add_to_sql(file, 'Daily', ercs)
            # if file.split('_')[2] == 'Nokia':
            #     if file.split('_')[4] == 'BBH':
            #         add_to_sql(file, 'BBH', nokia)
            #     elif file.split('_')[4] == 'NBH':
            #         add_to_sql(file, 'NBH', nokia)
            #     else:
            #         add_to_sql(file, 'Daily', nokia)
        else:
            continue

if __name__ == '__main__':
    main()
