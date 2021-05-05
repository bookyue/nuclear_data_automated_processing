from sqlalchemy import create_engine
from sqlalchemy.orm import registry
from sqlalchemy.orm import sessionmaker

from nuc_data_tool.utils.configlib import config


def _chosen_db(db_type=None, debug=False):
    """
    选择数据库（目前支持 Mysql, Postgresql, sqlite），生成对应的Session和engine

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
    db_config = config.get_database_config()

    if db_type is None:
        db_type = db_config['chosen_db']
    elif db_type not in db_config:
        db_type = 'sqlite'

    user = db_config[db_type].get('user')
    password = db_config[db_type].get('password')
    url = db_config[db_type].get('url')
    port = db_config[db_type].get('port')
    db_name = db_config[db_type].get('dbname')
    # sqlite
    path = db_config[db_type].get('path')

    if db_type == 'sqlite':
        connector_string = f'sqlite:///{path}'
        engine_tmp = create_engine(connector_string, future=True, echo=debug)
    elif db_type == 'mysql':
        connector_string = f'mysql+mysqlconnector://{user}:{password}@{url}:{port}/{db_name}?charset=utf8mb4'
        engine_tmp = create_engine(connector_string, future=True, echo=debug)
    elif db_type == 'postgresql':
        connector_string = f'postgresql+psycopg2://{user}:{password}@{url}:{port}/{db_name}?client_encoding=utf8'
        engine_tmp = create_engine(connector_string, future=True,
                                   executemany_mode='values',
                                   executemany_values_page_size=10000,
                                   executemany_batch_page_size=500,
                                   echo=debug)
    else:
        raise Exception(f"can't support {db_type} now")

    session_tmp = sessionmaker(bind=engine_tmp, future=True)

    return engine_tmp, session_tmp


mapper_registry = registry()
Base = mapper_registry.generate_base()
engine, Session = _chosen_db(debug=False)
