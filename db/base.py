from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from utils import configlib


def _choose_db(db_type, debug=False):
    """
    选择数据库（如Mysql, Postgresql），生成对应的Session和engine

    Parameters
    ----------
    db_type : str
        数据库类型
    debug : bool, default False
        是否开启echo
    Returns
    -------
    tuple
        返回对应数据裤的Session和engine
    """
    db_config = configlib.Config.get_database_config()

    if db_type not in db_config:
        db_type = 'mysql'
    user = db_config[db_type].get('user')
    password = db_config[db_type].get('password')
    url = db_config[db_type].get('url')
    port = db_config[db_type].get('port')
    db_name = db_config[db_type].get('dbname')

    if db_type == 'mysql':
        connector_string = f'mysql+mysqlconnector://{user}:{password}@{url}:{port}/{db_name}?charset=utf8mb4'
        engine_tmp = create_engine(connector_string, future=True, echo=debug)
    else:
        connector_string = f'postgresql+psycopg2://{user}:{password}@{url}:{port}/{db_name}?client_encoding=utf8'
        engine_tmp = create_engine(connector_string, future=True,
                                   executemany_mode='values',
                                   executemany_values_page_size=3500, executemany_batch_page_size=500,
                                   echo=debug)

    session_tmp = sessionmaker(bind=engine_tmp, future=True)

    return engine_tmp, session_tmp


Base = declarative_base()
engine, Session = _choose_db('postgresql', debug=False)
