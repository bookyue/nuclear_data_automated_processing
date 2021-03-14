from sqlalchemy import select, or_, insert

from db.base import Session
from db.db_model import File, Nuc, NucData, ExtractedData, PhysicalQuantity
from db.fetch_data import fetch_physical_quantities_by_name
from utils.physical_quantity_list_generator import is_it_all_str


def save_extracted_data_to_db(filename, physical_quantities, nuclide_list):
    """
    将数据存入到ExtractedData表里

    Parameters
    ----------
    filename : File
        File object
    physical_quantities: list or str
        核素名，可以是核素名的list或str，也可以是PhysicalQuantity list
    nuclide_list : list
        核素list
    """
    with Session() as session:
        if is_it_all_str(physical_quantities):
            physical_quantities = fetch_physical_quantities_by_name(physical_quantities)

        physical_quantity: PhysicalQuantity
        for physical_quantity in physical_quantities:
            file_id = filename.id
            physical_quantity_id = physical_quantity.id

            if nuclide_list is None:
                """核素列表为空则过滤first_step和last_step皆为0的records"""
                stmt = (select(NucData.nuc_id, NucData.file_id, NucData.physical_quantity_id, NucData.last_step,
                               NucData.middle_steps).
                        where(NucData.file_id == file_id, NucData.physical_quantity_id == physical_quantity_id).
                        where(or_(NucData.first_step != 0, NucData.last_step != 0))
                        )
            else:
                if physical_quantity.name != 'gamma_spectra':
                    """核素不为gamma时，依照核素列表过滤records，否则反之"""
                    stmt = (select(NucData.nuc_id, NucData.file_id, NucData.physical_quantity_id, NucData.last_step,
                                   NucData.middle_steps).
                            join(Nuc, Nuc.id == NucData.nuc_id).
                            where(NucData.file_id == file_id, NucData.physical_quantity_id == physical_quantity_id).
                            where(Nuc.name.in_(nuclide_list))
                            )
                else:
                    stmt = (select(NucData.nuc_id, NucData.file_id, NucData.physical_quantity_id, NucData.last_step,
                                   NucData.middle_steps).
                            where(NucData.file_id == file_id, NucData.physical_quantity_id == physical_quantity_id)
                            )
            # 用INSERT INTO FROM SELECT将数据插入ExtractedData table
            insert_stmt = insert(ExtractedData).from_select(
                names=['nuc_id', 'file_id', 'physical_quantity_id', 'last_step',
                       'middle_steps'],
                select=stmt)

            session.execute(insert_stmt)
            session.commit()
