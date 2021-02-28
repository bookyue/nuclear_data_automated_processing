from decimal import Decimal

from db.base import Session, engine, Base
from db.db_model import Nuc, NucData, File, PhysicalQuantity
from utils import configlib
from utils.input_xml_file import InputXmlFileReader


def init_db():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def populate_database(xml_file):
    # session = Session(autoflush=False)
    session = Session()

    nuc_data = []

    for key in xml_file.table_of_physical_quantity:

        # Generator
        # for line in (line for line in (xml_file.table_of_physical_quantity[key])):
        # for i, line in enumerate(xml_file.table_of_physical_quantity[key]):
        #     elements = line.split()
        #
        #     # nuc
        #     if key == 'gamma_spectra':
        #         nuc_id = None
        #         nuc_name = elements[0]
        #     else:
        #         nuc_id = elements[0]
        #         nuc_name = elements[1]
        #     nuc_tmp = (session.query(Nuc)
        #                .filter(Nuc.name == nuc_name)
        #                .one_or_none()
        #                )
        #     if nuc_tmp is None:
        #         nuc_tmp = Nuc(id=nuc_id, name=nuc_name)
        #         session.add(nuc_tmp)
        #
        #     # file
        #     file_tmp = (session.query(File)
        #                 .filter(File.name == xml_file.name)
        #                 .one_or_none()
        #                 )
        #     if file_tmp is None:
        #         file_tmp = File(name=xml_file.name)
        #         session.add(file_tmp)
        #
        #     # physical_quantity
        #     physical_quantity_tmp = (session.query(PhysicalQuantity)
        #                              .filter(PhysicalQuantity.name == key)
        #                              .one_or_none()
        #                              )
        #     if physical_quantity_tmp is None:
        #         physical_quantity_tmp = PhysicalQuantity(name=key)
        #         session.add(physical_quantity_tmp)
        #
        #     # nuc_data
        #     data_tmp = NucData(data1=Decimal(elements[-2]), data2=Decimal(elements[-1]))
        #
        #     nuc_tmp.data.append(data_tmp)
        #     file_tmp.data.append(data_tmp)
        #     file_tmp.physical_quantities.append(physical_quantity_tmp)
        #     physical_quantity_tmp.data.append(data_tmp)
        #     physical_quantity_tmp.files.append(file_tmp)
        #
        #     nuc_data.append(data_tmp)
        #
        #     if i % 1000 == 0:
        #         session.flush()
        #
        # session.add_all(nuc_data)
        for i, line in enumerate(xml_file.table_of_physical_quantity[key]):
            elements = line.split()

            # nuc
            if key == 'gamma_spectra':
                nuc_id = None
                nuc_name = elements[0]
            else:
                nuc_id = elements[0]
                nuc_name = elements[1]
            nuc_tmp = (session.query(Nuc)
                       .filter(Nuc.name == nuc_name)
                       .one_or_none()
                       )
            if nuc_tmp is None:
                nuc_tmp = Nuc(nuc_ix=nuc_id, name=nuc_name)
                session.add(nuc_tmp)

            # file
            file_tmp = (session.query(File)
                        .filter(File.name == xml_file.name)
                        .one_or_none()
                        )
            if file_tmp is None:
                file_tmp = File(name=xml_file.name)
                session.add(file_tmp)

            # physical_quantity
            physical_quantity_tmp = (session.query(PhysicalQuantity)
                                     .filter(PhysicalQuantity.name == key)
                                     .one_or_none()
                                     )
            if physical_quantity_tmp is None:
                physical_quantity_tmp = PhysicalQuantity(name=key)
                session.add(physical_quantity_tmp)

            # nuc_data
            data_tmp = NucData(data1=Decimal(elements[-2]), data2=Decimal(elements[-1]))

            nuc_tmp.data.append(data_tmp)
            file_tmp.data.append(data_tmp)
            file_tmp.physical_quantities.append(physical_quantity_tmp)
            physical_quantity_tmp.data.append(data_tmp)
            physical_quantity_tmp.files.append(file_tmp)

            session.add(data_tmp)

            if i % 1000 == 0:
                session.flush()

        session.commit()
    session.close()


def main():
    test_file_path = configlib.Config.get_file_path('test_file_path')
    physical_quantity_name = 'all'

    init_db()

    file_names = test_file_path.glob('*.out')
    for file_name in file_names:
        with InputXmlFileReader(file_name, physical_quantity_name) as xml_file:
            populate_database(xml_file)


if __name__ == '__main__':
    main()
