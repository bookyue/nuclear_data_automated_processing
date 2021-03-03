from decimal import Decimal

from sqlalchemy.orm import scoped_session

from db.base import Base, engine, session_factory
from db.db_model import Nuc, NucData, File, PhysicalQuantity
from utils import configlib
from utils.input_xml_file import InputXmlFileReader


def init_db():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def populate_database(session, xml_file):
    session_thread_local = session()

    # file
    file_tmp = (session_thread_local.query(File)
                .filter(File.name == xml_file.name)
                .one_or_none()
                )
    if file_tmp is None:
        file_tmp = File(name=xml_file.name)
        session_thread_local.add(file_tmp)

    for key in xml_file.table_of_physical_quantity:

        nuc_data = []

        # physical_quantity
        physical_quantity_tmp = (session_thread_local.query(PhysicalQuantity)
                                 .filter(PhysicalQuantity.name == key)
                                 .one_or_none()
                                 )
        if physical_quantity_tmp is None:
            physical_quantity_tmp = PhysicalQuantity(name=key)
            session_thread_local.add(physical_quantity_tmp)

        file_tmp.physical_quantities.append(physical_quantity_tmp)
        physical_quantity_tmp.files.append(file_tmp)

        for line in xml_file.table_of_physical_quantity[key]:
            elements = line.split()

            # nuc
            if key == 'gamma_spectra':
                nuc_id = None
                nuc_name = elements[0]
            else:
                nuc_id = elements[0]
                nuc_name = elements[1]
            nuc_tmp = (session_thread_local.query(Nuc)
                       .filter(Nuc.name == nuc_name)
                       .one_or_none()
                       )
            if nuc_tmp is None:
                nuc_tmp = Nuc(nuc_ix=nuc_id, name=nuc_name)
                session_thread_local.add(nuc_tmp)

            # nuc_data
            data_tmp = NucData(data1=Decimal(elements[-2]), data2=Decimal(elements[-1]))

            data_tmp.nuc = nuc_tmp
            data_tmp.file = file_tmp
            data_tmp.physical_quantity = physical_quantity_tmp

            nuc_data.append(data_tmp)

        session_thread_local.add_all(nuc_data)
        session_thread_local.commit()
    session_thread_local.close()


def main():
    test_file_path = configlib.Config.get_file_path('test_file_path')
    physical_quantity_name = 'all'
    init_db()
    session = scoped_session(session_factory)
    file_names = test_file_path.glob('*.out')
    for file_name in file_names:
        with InputXmlFileReader(file_name, physical_quantity_name) as xml_file:
            populate_database(session, xml_file)


if __name__ == '__main__':
    main()
