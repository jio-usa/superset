# https://medium.com/@vedashri.debray/setting-up-apache-superset-on-dockers-and-connecting-it-to-a-mysql-docker-container-a-foolproof-37fe5936ae83

# This file contains the script for adding the data to the MySQL database

import pandas as pd
import os
import pymysql
from sqlalchemy import create_engine

