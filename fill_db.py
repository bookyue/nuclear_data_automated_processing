from decimal import Decimal
import pandas as pd

import pangres
from sqlalchemy.dialects.mysql import insert

from db.base import Base, engine, Session
from db.db_model import Nuc, NucData, File, PhysicalQuantity
from utils import configlib
from utils.input_xml_file import InputXmlFileReader


def init_db():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def _upsert(model, data, update_field):
    """
    on_duplicate_key_update for mysql
    """
    stmt = insert(model).values(data)
    d = {f: getattr(stmt.inserted, f) for f in update_field}
    return stmt.on_duplicate_key_update(**d)


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

        nuc_data = []

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

        for line in xml_file.table_of_physical_quantity[key]:
            elements = line.split()

            # nuc
            if key == 'gamma_spectra':
                nuc_name = elements[0]
            else:
                nuc_name = elements[1]
            nuc_tmp = (session.query(Nuc)
                       .filter(Nuc.name == nuc_name)
                       .one_or_none()
                       )

            # nuc_data
            data_tmp = NucData(data1=Decimal(elements[-2]), data2=Decimal(elements[-1]))

            data_tmp.nuc = nuc_tmp
            data_tmp.file = file_tmp
            data_tmp.physical_quantity = physical_quantity_tmp

            nuc_data.append(data_tmp)

        session.add_all(nuc_data)
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
