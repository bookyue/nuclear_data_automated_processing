import pandas as pd
from sqlalchemy import select
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgres_insert

from db.base import Base, Session
from db.db_model import Nuc, NucData, File, PhysicalQuantity
from utils import configlib
from utils.input_xml_file import InputXmlFileReader
from utils.middle_steps import middle_steps_serialization


def init_db():
    session = Session()
    Base.metadata.drop_all(session.bind)
    Base.metadata.create_all(session.bind)


def _upsert(model, data, update_field):
    """
    on_duplicate_key_update for mysql
    on_conflict_do_nothing for postgresql
    """
    session = Session()
    if session.bind.dialect.name == 'mysql':
        stmt = mysql_insert(model).values(data)
        d = {f: getattr(stmt.inserted, f) for f in update_field}
        return stmt.on_duplicate_key_update(**d)
    elif session.bind.dialect.name == 'postgresql':
        stmt = postgres_insert(model).values(data)
        return stmt.on_conflict_do_nothing(index_elements=[update_field[0]])
    else:
        raise Exception(f"can't support {session.bind.dialect.name} dialect")


def populate_database(xml_file):
    session = Session()

    file_stmt = (select(File)
                 .where(File.name == xml_file.name)
                 )
    file_tmp = session.execute(file_stmt).scalar_one_or_none()
    if file_tmp is None:
        file_tmp = File(name=xml_file.name)
        session.add(file_tmp)

    for key in xml_file.table_of_physical_quantity:

        if not xml_file.table_of_physical_quantity[key]:
            continue

        physical_quantity_stmt = (select(PhysicalQuantity)
                                  .where(PhysicalQuantity.name == key)
                                  )
        physical_quantity_tmp = session.execute(physical_quantity_stmt).scalar_one_or_none()
        if physical_quantity_tmp is None:
            physical_quantity_tmp = PhysicalQuantity(name=key)
            session.add(physical_quantity_tmp)

        file_tmp.physical_quantities.append(physical_quantity_tmp)
        physical_quantity_tmp.files.append(file_tmp)

        df_all_tmp = pd.DataFrame(middle_steps_serialization(data.split())
                                  if key != 'gamma_spectra'
                                  else middle_steps_serialization([i, *data.split()])
                                  for i, data in enumerate(xml_file.table_of_physical_quantity[key])
                                  )

        df_nuc_tmp: pd.DataFrame = df_all_tmp.iloc[:, [0, 1]]

        df_nuc_tmp.columns = ('nuc_ix', 'name')
        stmt = _upsert(Nuc, df_nuc_tmp.to_dict(orient='records'), update_field=df_nuc_tmp.columns.values.tolist())
        session.execute(stmt)
        session.commit()

        if len(df_all_tmp.columns) != 5:
            df_data_tmp: pd.DataFrame = df_all_tmp.iloc[:, [-2, -1]]
            df_data_tmp.columns = ('first_step', 'last_step')
        else:
            df_data_tmp: pd.DataFrame = df_all_tmp.iloc[:, -3:]
            df_data_tmp.columns = ('first_step', 'last_step', 'middle_steps')

        if key == 'gamma_spectra':
            start = session.execute(select(Nuc.id).where(Nuc.nuc_ix == 0)).scalar()
        else:
            start = session.execute(select(Nuc.id).where(Nuc.nuc_ix == 10010)).scalar()

        df_data_prefix = pd.DataFrame({'nuc_id': range(start, len(df_nuc_tmp) + start),
                                       'file_id': file_tmp.id,
                                       'physical_quantity_id': physical_quantity_tmp.id})
        df_data_all = pd.concat([df_data_prefix, df_data_tmp],
                                axis=1, copy=False)

        # df_data_all.to_sql(name='nuc_data', con=engine, if_exists='append', index=False)
        # almost twice as slow as __table__.insert
        # session.execute(insert(NucData).values(df_data_all.to_dict(orient='records')))
        session.execute(NucData.__table__.insert(), df_data_all.to_dict(orient='records'))
        session.commit()

    session.close()


def main():
    test_file_path = configlib.Config.get_file_path('test_file_path')
    physical_quantity_name = 'all'
    init_db()

    # sqlite:      928.97s user 0.57s system 99% cpu 15:32.94 total
    # mysql:      1456.85s user 104.03s system 52% cpu 49:55.42 total
    # postgresql: 1010.68s user 31.02s system 67% cpu 25:52.97 total
    file_names = test_file_path.glob('*.out')
    for file_name in file_names:
        with InputXmlFileReader(file_name, physical_quantity_name) as xml_file:
            populate_database(xml_file)


if __name__ == '__main__':
    main()
