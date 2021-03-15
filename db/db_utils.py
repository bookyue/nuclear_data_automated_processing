from sqlalchemy import delete

from db.base import Session, Base


def init_db():
    """
    初始化数据库
    Returns
    -------

    """
    session = Session()
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
