from pathlib import Path

import pandas as pd

from db.db_model import File, PhysicalQuantity
from db.fetch_data import fetch_data_by_filename_and_nuclide_list, fetch_files_by_name, \
    fetch_physical_quantities_by_name, fetch_extracted_data_id, fetch_extracted_data_by_filename_and_physical_quantity
from utils.configlib import Config
from utils.formatter import type_checker
from utils.workbook import append_df_to_excel


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


def save_extracted_data_to_exel(nuc_data_id, filenames=None, is_all_step=False, dir_path=Path('.'), merge=True):
    """
    将数据存入到exel文件
    将传入的File list中包含的文件的数据存到exel文件
    如无filenames is None，则包含所有文件

    Parameters
    ----------
    nuc_data_id : list[int]
    filenames : comparison_files : list[File] or File
    is_all_step : bool, default = False
        是否读取全部中间结果数据列，默认只读取最终结果列
    dir_path : Path
    merge : bool, default = True
        是否将结果合并输出至一个文件，否则单独输出至每个文件

    Returns
    -------

    """

    if type_checker(filenames, File) == 'str':
        filenames = fetch_files_by_name(filenames)

    physical_quantities = fetch_physical_quantities_by_name('all')

    dir_path.mkdir(parents=True, exist_ok=True)

    file_path = dir_path.joinpath('final.xlsx')
    file_path.unlink(missing_ok=True)

    physical_quantity: PhysicalQuantity
    for physical_quantity in physical_quantities:
        df_left = pd.DataFrame(data=None, columns=['nuc_ix', 'name'])

        filename: File
        for filename in filenames:
            if not merge:
                file_path = dir_path.joinpath(f'{filename.name}.xlsx')
                file_path.unlink(missing_ok=True)

            df_right = fetch_extracted_data_by_filename_and_physical_quantity(nuc_data_id,
                                                                              filename,
                                                                              physical_quantity,
                                                                              is_all_step)

            if not df_right.empty:
                df_left = pd.merge(df_left, df_right, how='outer', on=['nuc_ix', 'name'])

            if not merge:
                append_df_to_excel(file_path, df_left,
                                   sheet_name=physical_quantity.name,
                                   index=False,
                                   encoding='utf-8')
                df_left = pd.DataFrame(data=None, columns=['nuc_ix', 'name'])

        if merge:
            append_df_to_excel(file_path, df_left,
                               sheet_name=physical_quantity.name,
                               index=False,
                               encoding='utf-8')


def process(filenames, physical_quantity_name, nuclide_list, is_all_step):
    physical_quantities = fetch_physical_quantities_by_name(physical_quantity_name)
    file_path = Config.get_file_path('result_file_path')

    nuc_data_id = fetch_extracted_data_id(filenames, physical_quantities, nuclide_list)
    save_extracted_data_to_exel(nuc_data_id, filenames, is_all_step, file_path, False)


def main():
    fission_light_nuclide_list = Config.get_nuclide_list('fission_light')
    add_nuclide_list = ['I135', 'Xe135', 'Cs135', 'Pm149',
                        'Sm149', 'Sm150', 'Pu239', 'U239',
                        'Np239', 'U233', 'Pa233']
    is_all_step = Config.get_data_extraction_conf('is_all_step')
    physical_quantity_name = 'all'
    filenames = fetch_files_by_name()

    process(filenames=filenames,
            physical_quantity_name=physical_quantity_name,
            nuclide_list=list(set(fission_light_nuclide_list + add_nuclide_list)),
            is_all_step=is_all_step)


if __name__ == '__main__':
    main()
