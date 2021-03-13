from sqlalchemy import select, or_, insert

from db.base import Session
from db.db_model import File, Nuc, NucData, ExtractedData
from db.fetch_data import fetch_physical_quantities_by_name


def save_extracted_data_to_db(filename: File, physical_quantities, nuclide_list):
    with Session() as session:
        if isinstance(physical_quantities, str):
            physical_quantities = fetch_physical_quantities_by_name(physical_quantities)

        for physical_quantity in physical_quantities:
            file_id = filename.id
            physical_quantity_id = physical_quantity.id

            if nuclide_list is None:
                stmt = (select(NucData.nuc_id, NucData.file_id, NucData.physical_quantity_id, NucData.last_step,
                               NucData.middle_steps).
                        where(NucData.file_id == file_id, NucData.physical_quantity_id == physical_quantity_id).
                        where(or_(NucData.first_step != 0, NucData.last_step != 0))
                        )
            else:
                if physical_quantity.name != 'gamma_spectra':
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

            insert_stmt = insert(ExtractedData).from_select(
                names=['nuc_id', 'file_id', 'physical_quantity_id', 'last_step',
                       'middle_steps'],
                select=stmt)

            session.execute(insert_stmt)
            session.commit()
