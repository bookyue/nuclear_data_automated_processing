from sqlalchemy import insert, delete
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgres_insert

from nuc_data_tool.db.base import Session, Base


def init_db():
    """
    初始化数据库

    Returns
    -------

    """
    with Session() as session:
        Base.metadata.drop_all(session.bind)
        Base.metadata.create_all(session.bind)


def delete_all_from_table(model):
    """
    删除某表的全部records

    Returns
    -------

    """
    with Session() as session:
        stmt = (delete(model))
        session.execute(stmt)
        session.commit()


def upsert(model, data, update_field, engine):
    """
    upsert实现
    依据session.bind.dialect得到当前数据库类型
    然后生成对应的upsert语句，当前支持mysql，postgresql，sqlite三种数据库
    on_duplicate_key_update for mysql
    on_conflict_do_nothing for postgresql
    insert or ignore for sqlite

    Parameters
    ----------
    model : Base
        orm model
    data : list
    update_field : list
    engine
        _engine.Engine instance
    Returns
    -------
    insert
        insert statement
    """

    if engine.dialect.name == 'mysql':
        stmt = mysql_insert(model).values(data)
        d = {f: getattr(stmt.inserted, f) for f in update_field}
        return stmt.on_duplicate_key_update(**d)
    elif engine.dialect.name == 'postgresql':
        stmt = postgres_insert(model).values(data)
        return stmt.on_conflict_do_nothing(index_elements=[update_field[0]])
    elif engine.dialect.name == 'sqlite':
        stmt = insert(model).values(data).prefix_with('OR IGNORE')
        return stmt
    else:
        raise Exception(f"can't support {engine.dialect.name} dialect")
