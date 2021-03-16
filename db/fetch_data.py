import pandas as pd
from sqlalchemy import select, lambda_stmt, or_

from db.base import Session
from db.db_model import File, NucData, Nuc, PhysicalQuantity
from utils.configlib import Config
from utils.middle_steps import middle_steps_line_parsing
from utils.physical_quantity_list_generator import physical_quantity_list_generator, is_it_all_str


def fetch_all_filenames():
    """
    获取files table中所有File objects

    Returns
    -------
    list[File]
        File list
    """
    with Session() as session:
        stmt = select(File)
        filenames = session.execute(stmt).scalars().all()
    return filenames


def fetch_physical_quantities_by_name(physical_quantities):
    """
    根据输入的物理量名，从physical_quantities table获取 physical_quantity(ies) object(s)

    Parameters
    ----------
    physical_quantities : list or str
        核素名，可以是核素名的list或str

    Returns
    -------
    list[PhysicalQuantity]
        PhysicalQuantity list

    See Also
    --------
    physical_quantity_list_generator : 根据输入生成对应的physical_quantity list
    """
    with Session() as session:
        physical_quantities_list = physical_quantity_list_generator(physical_quantities)
        stmt = (select(PhysicalQuantity)
                .where(PhysicalQuantity.name.in_(physical_quantities_list))
                )
        physical_quantities = session.execute(stmt).scalars().all()
    return physical_quantities


def fetch_data_by_filename(filename, physical_quantities):
    """
    根据输入的File和physical quantities从Nuc， NucData，PhysicalQuantity table获取数据

    Parameters
    ----------
    filename : File
        File object
    physical_quantities: list or str
        核素名，可以是核素名的list或str，也可以是PhysicalQuantity list
    Returns
    -------
    dict[str, pd.DataFrame]
        返回一个结果字典，key为物理量名(str)，value为对应物理量的数据(DataFrame)
    """
    dict_df_data = {}

    if is_it_all_str(physical_quantities):
        physical_quantities = fetch_physical_quantities_by_name(physical_quantities)

    with Session() as session:

        physical_quantity: PhysicalQuantity
        for physical_quantity in physical_quantities:
            stmt = (select(Nuc.nuc_ix, Nuc.name, NucData.first_step, NucData.last_step)
                    .join(Nuc, Nuc.id == NucData.nuc_id)
                    .join(PhysicalQuantity, PhysicalQuantity.id == NucData.physical_quantity_id)
                    .where(NucData.file_id == filename.id)
                    .where(PhysicalQuantity.id == physical_quantity.id)
                    )

            nuc_data = pd.DataFrame(data=session.execute(stmt).all(),
                                    columns=tuple(column.name
                                                  for column in list(stmt.selected_columns))
                                    )

            dict_df_data[physical_quantity.name] = nuc_data

    return dict_df_data


def fetch_data_by_filename_and_nuclide_list(filename, physical_quantities, nuclide_list, is_all_step=False):
    """
    根据输入的File，physical quantities，nuclide_list(核素列表)，all_step
    从Nuc， NucData，PhysicalQuantity table获取数据

    Parameters
    ----------
    filename : File
        File object
    physical_quantities: list or str
        核素名，可以是核素名的list或str，也可以是PhysicalQuantity list
    nuclide_list : list
        核素list
    is_all_step : bool, default false
        是否读取全部中间结果数据列，默认只读取最终结果列
    Returns
    -------
    dict[str, pd.DataFrame]
        返回一个结果字典，key为物理量名(str)，value为对应物理量的数据(DataFrame)
    """
    dict_df_data = {}

    if is_it_all_str(physical_quantities):
        physical_quantities = fetch_physical_quantities_by_name(physical_quantities)

    with Session() as session:
        physical_quantity: PhysicalQuantity
        for physical_quantity in physical_quantities:
            file_id = filename.id
            physical_quantity_id = physical_quantity.id

            if not is_all_step:
                # 不读取中间结果，所以不选择NucData.middle_steps，否则反之
                stmt = lambda_stmt(lambda: select(Nuc.nuc_ix, Nuc.name,
                                                  NucData.first_step, NucData.last_step))
            else:
                stmt = lambda_stmt(lambda: select(Nuc.nuc_ix, Nuc.name,
                                                  NucData.first_step, NucData.last_step,
                                                  NucData.middle_steps))

            stmt += lambda s: s.join(Nuc,
                                     Nuc.id == NucData.nuc_id)
            stmt += lambda s: s.join(PhysicalQuantity,
                                     PhysicalQuantity.id == NucData.physical_quantity_id)
            stmt += lambda s: s.where(NucData.file_id == file_id,
                                      PhysicalQuantity.id == physical_quantity_id)
            if nuclide_list is None:
                # 核素列表为空则过滤first_step和last_step皆为0的records
                stmt += lambda s: s.where(or_(NucData.first_step != 0, NucData.last_step != 0))
            else:
                if physical_quantity.name != 'gamma_spectra':
                    # 核素不为gamma时，依照核素列表过滤records，否则反之
                    stmt += lambda s: s.where(Nuc.name.in_(nuclide_list))

            nuc_data = pd.DataFrame(data=session.execute(stmt).all(),
                                    columns=tuple(column.name for
                                                  column in list(stmt.selected_columns))
                                    )

            if is_all_step:
                nuc_data_exclude_middle_steps = nuc_data.drop(columns='middle_steps', axis=1)
                middle_steps = pd.DataFrame([middle_steps_line_parsing(middle_steps)
                                             for middle_steps in nuc_data['middle_steps']
                                             if middle_steps is not None])

                del nuc_data
                nuc_data = pd.concat([nuc_data_exclude_middle_steps, middle_steps],
                                     axis=1, copy=False)

            dict_df_data[physical_quantity.name] = nuc_data

    return dict_df_data


def main():
    filenames = fetch_all_filenames()
    fission_light_nuclide_list = Config.get_nuclide_list("fission_light")
    dict_df_data = fetch_data_by_filename_and_nuclide_list(filenames[32], ['isotope', 'radioactivity'],
                                                           fission_light_nuclide_list, True)
    print(dict_df_data)


if __name__ == '__main__':
    main()
