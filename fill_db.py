from decimal import Decimal

import pandas as pd
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgres_insert

from db.base import Base, Session
from db.db_model import Nuc, NucData, File, PhysicalQuantity
from utils import configlib
from utils.input_xml_file import InputXmlFileReader


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

    # file
    file_tmp = (session.query(File)
                .filter(File.name == xml_file.name)
                .one_or_none()
                )
    if file_tmp is None:
        file_tmp = File(name=xml_file.name)
        session.add(file_tmp)

    for key in xml_file.table_of_physical_quantity:

        if not xml_file.table_of_physical_quantity[key]:
            continue

        # physical_quantity
        physical_quantity_tmp = (session.query(PhysicalQuantity)
                                 .filter(PhysicalQuantity.name == key)
                                 .one_or_none()
                                 )
        if physical_quantity_tmp is None:
            physical_quantity_tmp = PhysicalQuantity(name=key)
            session.add(physical_quantity_tmp)

        file_tmp.physical_quantities.append(physical_quantity_tmp)
        physical_quantity_tmp.files.append(file_tmp)

        df_all_tmp = pd.DataFrame(data.split() for data in xml_file.table_of_physical_quantity[key])

        if key == 'gamma_spectra':
            df_nuc_tmp: pd.DataFrame = df_all_tmp.iloc[:, [0]]
            df_nuc_tmp.insert(0, 'nuc_ix', range(len(df_nuc_tmp)))
        else:
            df_nuc_tmp: pd.DataFrame = df_all_tmp.iloc[:, [0, 1]]

        df_nuc_tmp.columns = ('nuc_ix', 'name')
        stmt = _upsert(Nuc, df_nuc_tmp.to_dict(orient='records'), update_field=df_nuc_tmp.columns.values.tolist())
        session.execute(stmt)
        session.commit()

        df_data_tmp: pd.DataFrame = df_all_tmp.iloc[:, [-2, -1]]
        df_data_tmp.applymap(Decimal)
        df_data_tmp.columns = ('data1', 'data2')

        if key == 'gamma_spectra':
            start = session.query(Nuc.id).filter(Nuc.nuc_ix == 0).scalar()
        else:
            start = session.query(Nuc.id).filter(Nuc.nuc_ix == 10010).scalar()

        df_data_prefix = pd.DataFrame({'nuc_id': range(start, len(df_nuc_tmp) + start),
                                       'file_id': file_tmp.id,
                                       'physical_quantity_id': physical_quantity_tmp.id})
        df_data_all = pd.concat([df_data_prefix, df_data_tmp],
                                axis=1, copy=False)

        # df_data_all.to_sql(name='nuc_data', con=engine, if_exists='append', index=False)
        session.execute(NucData.__table__.insert(), df_data_all.to_dict(orient='records'))
        session.commit()

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
