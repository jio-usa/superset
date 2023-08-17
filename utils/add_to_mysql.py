# https://medium.com/@vedashri.debray/setting-up-apache-superset-on-dockers-and-connecting-it-to-a-mysql-docker-container-a-foolproof-37fe5936ae83

# This file contains the script for adding the data to the MySQL database
# To restart container, enter `docker run atoc` in terminal

import pandas as pd
import os
import pymysql
from sqlalchemy import create_engine
from tqdm import tqdm
from logging import getLogger

logger = getLogger(__name__)

DATA_DIR = '/media/labuser/29ecfc4a-5bf9-4b2f-8fc7-0733eaef8266/nocAi/data/raw/backup'

# mysql+pymysql://root:atoc@localhost:3306/network_kpis_ercs

with open('sql_uris.txt', 'r') as f:
    ercs = create_engine(f.readline().strip())
    nokia = create_engine(f.readline().strip())

kpis = pd.read_excel('kpi_list.xlsx')
kpis = kpis['KPINAME'].tolist()

def read_and_preprocess(file):
    """
    Preprocess the data to only use the required columns
    """
    df = pd.read_csv(os.path.join(DATA_DIR, file))
    k = [i for i in df.columns if i in kpis]
    k = k + df.columns[:12].tolist()
    df = df[k]
    return df

def debug_purge(delete=False;):
    """
    Delete the data from the MySQL database
    """
    if delete:
        with ercs.connect() as conn:
            conn.execute("DROP TABLE BBH_ERCS;")
            conn.execute("DROP TABLE NBH_ERCS;")
            conn.execute("DROP TABLE Daily_ERCS;")
        with nokia.connect() as conn:
            conn.execute("DROP TABLE BBH_Nokia;")
            conn.execute("DROP TABLE NBH_Nokia;")
            conn.execute("DROP TABLE Daily_Nokia;")

def add_to_sql(file, table_name, engine):
    """
    Add the data to the MySQL database
    """
    df = read_and_preprocess(file)
    df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=1000)
    df = pd.DataFrame()

def main():
    """
    Main function
    """
    # debug_purge()
    for file in tqdm(os.listdir(DATA_DIR)):
        if file.endswith(".csv"):
            if file.split('_')[2] == 'ERCS':
                if file.split('_')[4] == 'BBH':
                    add_to_sql(file, 'BBH_ERCS', ercs)
                elif file.split('_')[4] == 'NBH':
                    add_to_sql(file, 'NBH_ERCS', ercs)
                else:
                    add_to_sql(file, 'Daily_ERCS', ercs)
            if file.split('_')[2] == 'Nokia':
                if file.split('_')[4] == 'BBH':
                    add_to_sql(file, 'BBH_Nokia', nokia)
                elif file.split('_')[4] == 'NBH':
                    add_to_sql(file, 'NBH_Nokia', nokia)
                else:
                    add_to_sql(file, 'Daily_Nokia', nokia)
        else:
            continue

if __name__ == '__main__':
    main()
