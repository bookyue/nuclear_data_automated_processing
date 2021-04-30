from pathlib import Path

import pandas as pd

from nuc_data_tool.db.db_model import File, PhysicalQuantity
from nuc_data_tool.db.fetch_data import (fetch_data_by_filename_and_nuclide_list, fetch_files_by_name,
                                         fetch_extracted_data_by_filename_and_physical_quantity,
                                         fetch_physical_quantities_by_name)
from nuc_data_tool.utils.formatter import type_checker
from nuc_data_tool.utils.workbook import save_to_excel


def filter_data(filename, physical_quantity_name, nuclide_list, is_all_step):
    """
    依据核素名称，筛选数据行
    :param is_all_step:
    :param filename: File object
    :param physical_quantity_name: str or list or PhysicalQuantity object
    :param nuclide_list: list, 关键核素，如果为None，则取不为0的全部核素
    :return: DataFrame，关键核素的计算结果
    """
    # if (nuclide_list is not None) and (len(nuclide_list) > 1):
    #     for key in dict_df_data:
    #         if key == 'gamma_spectra':
    #             continue
    #         is_in_nuclide_list = dict_df_data[key]['nuc_name'].isin(nuclide_list)
    #         dict_df_data[key]: pd.DataFrame = dict_df_data[key].loc[is_in_nuclide_list]
    # else:
    #     # Drop rows with all zeros in data.
    #     for key in dict_df_data:
    #         """
    #         The two lines below, does the exact same thing here.
    #         Drop rows with all zeros. But the latter one is way much faster.
    #         df_filter = df_data.iloc[df_data.apply(np.sum, axis=1).to_numpy().nonzero()]
    #         df_filter = df_data.iloc[df_data.any(axis=1).to_numpy().nonzero()]
    #
    #         When only condition is provided,
    #         the numpy.where() function is a shorthand for np.asarray(condition).nonzero().
    #         Using nonzero directly should be preferred, as it behaves correctly for subclasses.
    #         The rest of this documentation covers only the case where all three arguments are provided.
    #         But to be clear, I leave the np.where() kind of function in this comment.
    #         df_is_not_zero = np.where(df_density.any(axis=1))
    #         """
    #
    #         df_is_not_zero = dict_df_data[key][['first_step', 'last_step']].any(axis=1).to_numpy().nonzero()
    #         dict_df_data[key] = dict_df_data[key].iloc[df_is_not_zero]
    #         # df_output = df_data.loc[(df_density != Decimal(0)).any(axis=1), :]
    dict_df_data = fetch_data_by_filename_and_nuclide_list(filename, physical_quantity_name, nuclide_list, is_all_step)

    return dict_df_data


def save_extracted_data_to_exel(nuc_data_id,
                                filenames=None,
                                physical_quantities=None,
                                is_all_step=False,
                                result_path=Path('.'),
                                merge=True):
    """
    将数据存入到exel文件
    将传入的File list中包含的文件的数据存到exel文件
    如无filenames is None，则包含所有文件

    Parameters
    ----------
    nuc_data_id : list[int]
    filenames : list[File or str] or File or str
    physical_quantities : list[str or PhysicalQuantity] or str or PhysicalQuantity
        物理量，可以是物理量名的list[str]或str，
        也可以是list[PhysicalQuantity]或PhysicalQuantity
    is_all_step : bool, default = False
        是否读取全部中间结果数据列，默认只读取最终结果列
    result_path : Path or str
    merge : bool, default = True
        是否将结果合并输出至一个文件，否则单独输出至每个文件

    Returns
    -------

    """

    if type_checker(filenames, File) == 'str':
        filenames = fetch_files_by_name(filenames)

    if type_checker(physical_quantities, PhysicalQuantity) == 'str':
        physical_quantities = fetch_physical_quantities_by_name(physical_quantities)

    file_name = 'final.xlsx'

    if is_all_step:
        file_name = f'all_steps_{file_name}'

    if merge:
        Path(result_path).joinpath(file_name).unlink(missing_ok=True)
    else:
        for filename in filenames:
            if is_all_step:
                Path(result_path).joinpath(f'all_steps_{filename.name}.xlsx').unlink(missing_ok=True)
            else:
                Path(result_path).joinpath(f'{filename.name}.xlsx').unlink(missing_ok=True)
        del filename

    physical_quantity: PhysicalQuantity
    for physical_quantity in physical_quantities:
        df_left = pd.DataFrame(data=None, columns=['nuc_ix', 'name'])

        filename: File
        for filename in filenames:

            files_name = f'{filename.name}.xlsx'
            if is_all_step:
                files_name = f'all_steps_{filename.name}.xlsx'

            df_right = fetch_extracted_data_by_filename_and_physical_quantity(nuc_data_id,
                                                                              filename,
                                                                              physical_quantity,
                                                                              is_all_step)

            if not df_right.empty:
                df_left = pd.merge(df_left, df_right, how='outer', on=['nuc_ix', 'name'])

            if not merge:
                save_to_excel({physical_quantity.name: df_left},
                              files_name,
                              result_path)
                df_left = pd.DataFrame(data=None, columns=['nuc_ix', 'name'])

        if merge:
            save_to_excel({physical_quantity.name: df_left},
                          file_name,
                          result_path)
