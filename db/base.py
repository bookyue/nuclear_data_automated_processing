from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

from utils import configlib

# create an engine
sqlite_filepath = configlib.Config.get_database_config('path')
# SQLite
# engine = create_engine(f'sqlite:///{sqlite_filepath}', echo=True)
# mysql
engine = create_engine('mysql+mysqlconnector://user:password@localhost:3306/db?charset=utf8mb4', echo=True)
# postgres
# engine = create_engine('postgresql+psycopg2://user:password@localhost:5432/db?client_encoding=utf8',
#                        executemany_mode='batch')

# create a configured "Session" class
Session = sessionmaker(bind=engine)

Base = declarative_base()
