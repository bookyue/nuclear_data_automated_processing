from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from utils import configlib

# create an engine
sqlite_filepath = configlib.Config.get_database_config('path')
engine = create_engine(f'sqlite:///{sqlite_filepath}')

# create a configured "Session" class
Session = sessionmaker(bind=engine)

Base = declarative_base()
