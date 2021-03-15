from sqlalchemy import delete

from db.base import Session, Base


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
