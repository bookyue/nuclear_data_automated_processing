import pandas as pd
from sqlalchemy import select, lambda_stmt, or_

from db.base import Session
from db.db_model import File, NucData, Nuc, PhysicalQuantity
from utils.configlib import Config
from utils.middle_steps import middle_steps_parsing
from utils.physical_quantity_list_generator import physical_quantity_list_generator


def fetch_all_filenames():
    with Session() as session:
        stmt = select(File)
        filenames = session.execute(stmt).scalars().all()
    return filenames


def fetch_physical_quantities_by_name(physical_quantities: str):
    with Session() as session:
        physical_quantities_list = physical_quantity_list_generator(physical_quantities)
        stmt = (select(PhysicalQuantity)
                .where(PhysicalQuantity.name.in_(physical_quantities_list))
                )
        physical_quantities = session.execute(stmt).scalars().all()
    return physical_quantities


def fetch_data_by_filename(filename: File, physical_quantities):
    dict_df_data = {}
    with Session() as session:
        if isinstance(physical_quantities, str):
            physical_quantities = fetch_physical_quantities_by_name(physical_quantities)

        for physical_quantity in physical_quantities:
            stmt = (select(Nuc.nuc_ix, Nuc.name, NucData.first_step, NucData.last_step)
                    .join(Nuc, Nuc.id == NucData.nuc_id)
                    .join(PhysicalQuantity, PhysicalQuantity.id == NucData.physical_quantity_id)
                    .where(NucData.file_id == filename.id)
                    .where(PhysicalQuantity.id == physical_quantity.id)
                    )

            nuc_data = pd.DataFrame(data=session.execute(stmt).all(),
                                    columns=('nuc_ix', 'nuc_name', 'first_step', 'last_step'),
                                    )

            dict_df_data[physical_quantity.name] = nuc_data

    return dict_df_data


def fetch_data_by_filename_and_nuclide_list(filename: File, physical_quantities, nuclide_list):
    dict_df_data = {}
    with Session() as session:
        if isinstance(physical_quantities, str):
            physical_quantities = fetch_physical_quantities_by_name(physical_quantities)

        stmt = lambda_stmt(lambda: select(Nuc.nuc_ix, Nuc.name,
                                          NucData.first_step, NucData.last_step, NucData.middle_steps))
        stmt += lambda s: s.join(Nuc, Nuc.id == NucData.nuc_id)
        stmt += lambda s: s.join(PhysicalQuantity, PhysicalQuantity.id == NucData.physical_quantity_id)

        for physical_quantity in physical_quantities:
            file_id = filename.id
            physical_quantity_id = physical_quantity.id

            stmt += lambda s: s.where(NucData.file_id == file_id,
                                      PhysicalQuantity.id == physical_quantity_id)
            if nuclide_list is None:
                stmt += lambda s: s.where(or_(NucData.first_step != 0, NucData.last_step != 0))

            else:
                if physical_quantity.name != 'gamma_spectra':
                    stmt += lambda s: s.where(Nuc.name.in_(nuclide_list))

            nuc_data = pd.DataFrame(data=session.execute(stmt).all(),
                                    columns=('nuc_ix', 'nuc_name', 'first_step', 'last_step', 'middle_steps')
                                    )

            nuc_data_exclude_middle_steps = nuc_data.drop(columns='middle_steps', axis=1)
            middle_steps = pd.DataFrame([middle_steps_parsing(middle_steps)
                                         for middle_steps in nuc_data['middle_steps']
                                         if middle_steps is not None
                                         ])

            del nuc_data
            nuc_data = pd.concat([nuc_data_exclude_middle_steps, middle_steps], axis=1, copy=False)
            dict_df_data[physical_quantity.name] = nuc_data

    return dict_df_data


def main():
    filenames = fetch_all_filenames()
    fission_light_nuclide_list = Config.get_nuclide_list("fission_light")
    dict_df_data = fetch_data_by_filename_and_nuclide_list(filenames[1], 'all', fission_light_nuclide_list)
    print(dict_df_data)


if __name__ == '__main__':
    main()
