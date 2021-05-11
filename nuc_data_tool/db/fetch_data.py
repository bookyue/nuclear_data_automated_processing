import pandas as pd
from sqlalchemy import select, lambda_stmt, or_

from nuc_data_tool.db.base import Session
from nuc_data_tool.db.db_model import File, NucData, Nuc, PhysicalQuantity
from nuc_data_tool.utils.formatter import physical_quantity_list_generator, type_checker
from nuc_data_tool.utils.middle_steps import middle_steps_line_parsing


def fetch_files_by_name(filenames='all'):
    """
    根据输入的物理量名，从files table获取 file object(s)

    Parameters
    ----------
    filenames : str or list[str]

    Returns
    -------
    list[File]
    """
    stmt = lambda_stmt(lambda: select(File))
    if (filenames == 'all') or ('all' in list(filenames)):
        pass
    else:
        if isinstance(filenames, str):
            filenames = [filenames]

        stmt += lambda s: s.where(File.name.in_(filenames))

    try:
        with Session() as session:
            files = session.execute(stmt).scalars().all()
    finally:
        if not files:
            raise Exception(f"{filenames} doesn't exists")

    return files


def fetch_physical_quantities_by_name(physical_quantities):
    """
    根据输入的物理量名，从physical_quantities table获取 physical_quantity object(s)

    Parameters
    ----------
    physical_quantities : list[str] or str
        核素名，可以是核素名的list或str

    Returns
    -------
    list[PhysicalQuantity]

    See Also
    --------
    physical_quantity_list_generator : 根据输入生成对应的物理量list
    physical_quantity list
    """
    with Session() as session:
        physical_quantities_list = physical_quantity_list_generator(physical_quantities)
        stmt = (select(PhysicalQuantity)
                .where(PhysicalQuantity.name.in_(physical_quantities_list))
                )
        physical_quantities = session.execute(stmt).scalars().all()

    return physical_quantities


def fetch_data_by_filename_and_physical_quantities(filename, physical_quantities, is_all_step=False):
    """
    根据输入的File和physical quantities从Nuc， NucData，PhysicalQuantity table获取数据

    Parameters
    ----------
    filename : File
        File object
    physical_quantities : list[str] or str or list[PhysicalQuantity] or PhysicalQuantity
        物理量，可以是物理量名的list[str]或str，
        也可以是PhysicalQuantity list也可以是list[PhysicalQuantity]或PhysicalQuantity
    is_all_step : bool, default false
        是否读取全部中间结果数据列，默认只读取最终结果列

    Returns
    -------
    dict[str, pd.DataFrame]
        返回一个结果字典，key为物理量名(str)，value为对应物理量的数据(DataFrame)
    """
    dict_df_data = {}

    if type_checker(filename, File) == 'str':
        filename = fetch_files_by_name(filename).pop()

    if type_checker(physical_quantities, PhysicalQuantity) == 'str':
        physical_quantities = fetch_physical_quantities_by_name(physical_quantities)

    physical_quantity: PhysicalQuantity
    for physical_quantity in physical_quantities:
        nuc_data = fetch_data_by_filename_and_physical_quantity(filename,
                                                                physical_quantity,
                                                                is_all_step)
        dict_df_data[physical_quantity.name] = nuc_data

    return dict_df_data


def fetch_data_by_filename_and_physical_quantity(filename, physical_quantity, is_all_step=False):
    """
    根据输入的 File 和 physical quantity 从 Nuc， NucData，PhysicalQuantity table获取数据

    Parameters
    ----------
    filename : File
        File object
    physical_quantity : str or PhysicalQuantity
        物理量，可以是物理量的 str，PhysicalQuantity
    is_all_step : bool, default false
        是否读取全部中间结果数据列，默认只读取最终结果列

    Returns
    -------
    pd.DataFrame
    """

    if type_checker(filename, File) == 'str':
        filename = fetch_files_by_name(filename).pop()

    if type_checker(physical_quantity, PhysicalQuantity) == 'str':
        physical_quantity = fetch_physical_quantities_by_name(physical_quantity).pop()

    df_left = pd.DataFrame(data=None, columns=['nuc_ix', 'name'])

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

    with Session() as session:
        column_names = [column.name for
                        column in list(stmt.selected_columns)]
        df_right = pd.DataFrame(data=session.execute(stmt).all(),
                                columns=column_names)
        if is_all_step:
            exclude_middle_steps = df_right.drop(columns='middle_steps', axis=1)
            del column_names[-1]
            exclude_middle_steps.columns = column_names

            middle_steps = pd.DataFrame([middle_steps_line_parsing(middle_steps)
                                         for middle_steps in df_right['middle_steps']
                                         if middle_steps is not None])

            df_right = pd.concat([exclude_middle_steps, middle_steps], axis=1, copy=False)

    if not df_right.empty:
        df_left = pd.merge(df_left, df_right, how='outer', on=['nuc_ix', 'name'])

    df_left.sort_values(by=['nuc_ix'], inplace=True)

    return df_left


def fetch_data_by_filename_and_nuclide_list(filename, physical_quantities, nuclide_list, is_all_step=False):
    """
    根据输入的File，physical quantities，nuclide_list(核素列表)，all_step
    从Nuc， NucData，PhysicalQuantity table获取数据

    Parameters
    ----------
    filename : File
        File object
    physical_quantities : list[str] or str or list[PhysicalQuantity] or PhysicalQuantity
        物理量，可以是物理量名的list[str]或str，
        也可以是PhysicalQuantity list也可以是list[PhysicalQuantity]或PhysicalQuantity
    nuclide_list : list[str]
        核素list
    is_all_step : bool, default false
        是否读取全部中间结果数据列，默认只读取最终结果列

    Returns
    -------
    dict[str, pd.DataFrame]
        返回一个结果字典，key为物理量名(str)，value为对应物理量的数据(DataFrame)
    """
    dict_df_data = {}

    if type_checker(physical_quantities, PhysicalQuantity) == 'str':
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

            nuc_data.sort_values(by=['nuc_ix'], inplace=True)
            dict_df_data[physical_quantity.name] = nuc_data

    return dict_df_data


def fetch_extracted_data_id(filenames=None, physical_quantities='all', nuclide_list=None):
    """
    获取extracted_data的id

    Parameters
    ----------
    filenames : list[File] or File
        File object
    physical_quantities : list[str or PhysicalQuantity] or str or PhysicalQuantity
        物理量，可以是物理量名的list[str]或str，
        也可以是list[PhysicalQuantity]或PhysicalQuantity
    nuclide_list : list[str]
        核素list

    Returns
    -------
    list[int]
    """

    if type_checker(filenames, File) == 'str':
        filenames = fetch_files_by_name(filenames)

    if not isinstance(filenames, list):
        filenames = [filenames]

    if type_checker(physical_quantities, PhysicalQuantity) == 'str':
        physical_quantities = fetch_physical_quantities_by_name(physical_quantities)

    nuc_data_id = []

    with Session() as session:
        for filename in filenames:
            physical_quantities_id = [physical_quantity.id
                                      for physical_quantity in physical_quantities]
            file_id = filename.id

            if nuclide_list is None:
                # 核素列表为空则过滤first_step和last_step皆为0的records
                stmt = (select(NucData.id).
                        where(NucData.file_id == file_id,
                              NucData.physical_quantity_id.in_(physical_quantities_id)).
                        where(or_(NucData.first_step != 0, NucData.last_step != 0))
                        )
            else:
                # 核素不为gamma时，依照核素列表过滤records，否则反之
                for physical_quantity in physical_quantities:
                    if physical_quantity.name == 'gamma_spectra':
                        gamma_physical_quantity_id = physical_quantity.id

                        gamma_stmt = (select(NucData.id).
                                      where(NucData.file_id == file_id,
                                            NucData.physical_quantity_id == gamma_physical_quantity_id)
                                      )
                        nuc_data_id.extend(session.execute(gamma_stmt).scalars().all())

                stmt = (select(NucData.id).
                        join(Nuc, Nuc.id == NucData.nuc_id).
                        where(NucData.file_id == file_id,
                              NucData.physical_quantity_id.in_(physical_quantities_id)).
                        where(Nuc.name.in_(nuclide_list))
                        )

            nuc_data_id.extend(session.execute(stmt).scalars().all())

    return nuc_data_id


def fetch_extracted_data_by_filename_and_physical_quantity(nuc_data_id,
                                                           filename,
                                                           physical_quantity,
                                                           is_all_step=False):
    """
    获取 extracted_data

    Parameters
    ----------
    nuc_data_id : list[int]
    filename :str or File
    physical_quantity : str or PhysicalQuantity
    is_all_step : bool, default = False
        是否读取全部中间结果数据列，默认只读取最终结果列

    Returns
    -------

    """

    if type_checker(filename, File) == 'str':
        filename = fetch_files_by_name(filename).pop()

    if type_checker(physical_quantity, PhysicalQuantity) == 'str':
        physical_quantity = fetch_physical_quantities_by_name(physical_quantity).pop()

    df_left = pd.DataFrame(data=None, columns=['nuc_ix', 'name'])

    physical_quantity_id = physical_quantity.id

    filename: File
    file_id = filename.id

    if not is_all_step:
        # 不读取中间结果，所以不选择NucData.middle_steps，否则反之
        stmt = lambda_stmt(lambda: select(Nuc.nuc_ix,
                                          Nuc.name,
                                          NucData.last_step).
                           where(NucData.id.in_(nuc_data_id)))
    else:
        stmt = lambda_stmt(lambda: select(Nuc.nuc_ix,
                                          Nuc.name,
                                          NucData.last_step,
                                          NucData.middle_steps).
                           where(NucData.id.in_(nuc_data_id)))

    stmt += lambda s: s.join(Nuc,
                             Nuc.id == NucData.nuc_id)
    stmt += lambda s: s.join(PhysicalQuantity,
                             PhysicalQuantity.id == NucData.physical_quantity_id)
    stmt += lambda s: s.where(NucData.file_id == file_id,
                              PhysicalQuantity.id == physical_quantity_id)

    with Session() as session:
        if not is_all_step:
            column_names = ['nuc_ix', 'name', f'{filename.name}_last_step']
            df_right = pd.DataFrame(data=session.execute(stmt).all(),
                                    columns=column_names)
        else:
            column_names = ['nuc_ix', 'name', f'{filename.name}_last_step', 'middle_steps']
            df_right = pd.DataFrame(data=session.execute(stmt).all(),
                                    columns=column_names)

            exclude_middle_steps = df_right.drop(columns='middle_steps', axis=1)
            del column_names[-1]
            exclude_middle_steps.columns = column_names

            middle_steps = pd.DataFrame([middle_steps_line_parsing(middle_steps)
                                         for middle_steps in df_right['middle_steps']
                                         if middle_steps is not None])
            middle_step_column_names = [f'{filename.name}_{name}'
                                        for name in middle_steps.columns.tolist()]
            middle_steps.columns = middle_step_column_names

            df_right = pd.concat([exclude_middle_steps, middle_steps], axis=1, copy=False)

    if not df_right.empty:
        df_left = pd.merge(df_left, df_right, how='outer', on=['nuc_ix', 'name'])

    df_left.sort_values(by=['nuc_ix'], inplace=True)

    return df_left


def fetch_max_num_of_middle_steps(physical_quantity='isotope'):
    """
    获取选定物理量中所有文件 middle_step 的最大值

    Parameters
    ----------
    physical_quantity : str or PhysicalQuantity, default = 'isotope'
        物理量，可以是物理量名的list[str]或str，
        默认为核素密度

    Returns
    -------
    int
    """
    files = fetch_files_by_name(filenames='all')

    if type_checker(physical_quantity, PhysicalQuantity) == 'str':
        physical_quantity = fetch_physical_quantities_by_name(physical_quantity).pop()

    max_num = 0
    for file in files:
        nuc_data = fetch_data_by_filename_and_physical_quantity(file,
                                                                physical_quantity,
                                                                True)
        cur_num = len(nuc_data.columns)
        if max_num < cur_num:
            max_num = cur_num

    return max_num - 4
