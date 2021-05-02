from pathlib import Path

import pandas as pd
from pycaret.anomaly import load_model, predict_model

from nuc_data_tool.db.db_model import File, PhysicalQuantity
from nuc_data_tool.db.fetch_data import (fetch_files_by_name,
                                         fetch_physical_quantities_by_name,
                                         fetch_data_by_filename_and_physical_quantity)
from nuc_data_tool.utils.formatter import type_checker
from nuc_data_tool.utils.workbook import save_to_excel


def iforest_prediction(filename,
                       physical_quantity='isotope',
                       is_all_step=False,
                       model=None):
    """

    Parameters
    ----------
    filename : File or str
    physical_quantity : str or PhysicalQuantity, default = 'isotope'
        物理量，可以是物理量名的list[str]或str，
        默认为核素密度
    is_all_step : bool, default = False
        是否读取全部中间结果数据列，默认只读取最终结果列
    model

    Returns
    -------
    pd.DataFrame
    """

    if type_checker(filename, File) == 'str':
        filename = fetch_files_by_name(filename).pop()

    if type_checker(physical_quantity, PhysicalQuantity) == 'str':
        physical_quantity = fetch_physical_quantities_by_name(physical_quantity).pop()

    nuc_data = fetch_data_by_filename_and_physical_quantity(filename, physical_quantity, is_all_step)

    if nuc_data.empty:
        return nuc_data

    prediction = predict_model(model, data=nuc_data)

    return prediction[prediction['Label'] == 1]


def save_prediction_to_exel(filenames,
                            physical_quantities='isotope',
                            is_all_step=False,
                            result_path=Path('.'),
                            merge=True,
                            model_name='NUC IForest Model'):
    """

    Parameters
    ----------
    filenames : list[File or str] or File or str
    physical_quantities : list[str or PhysicalQuantity] or str or PhysicalQuantity
        物理量，可以是物理量名的list[str]或str，
        也可以是list[PhysicalQuantity]或PhysicalQuantity
    is_all_step : bool, default = False
        是否读取全部中间结果数据列，默认只读取最终结果列
    result_path : Path or str
    merge : bool, default = True
        是否将结果合并输出至一个文件，否则单独输出至每个文件
    model_name : str

    Returns
    -------

    """

    if type_checker(filenames, File) == 'str':
        filenames = fetch_files_by_name(filenames)

    if type_checker(physical_quantities, PhysicalQuantity) == 'str':
        physical_quantities = fetch_physical_quantities_by_name(physical_quantities)

    model = load_model(model_name)

    prefix = 'iforest'

    file_name = 'final.xlsx'

    if is_all_step:
        file_name = f'all_steps_{file_name}'

    file_name = f'{prefix}_{file_name}'

    if merge:
        Path(result_path).joinpath(file_name).unlink(missing_ok=True)
    else:
        for filename in filenames:
            if is_all_step:
                Path(result_path).joinpath(f'{prefix}_all_steps_{filename.name}.xlsx').unlink(missing_ok=True)
            else:
                Path(result_path).joinpath(f'{prefix}_{filename.name}.xlsx').unlink(missing_ok=True)
        del filename

    for physical_quantity in physical_quantities:
        df_left = pd.DataFrame(data=None, columns=['nuc_ix', 'name'])

        for filename in filenames:

            files_name = f'{prefix}_{filename.name}.xlsx'
            if is_all_step:
                files_name = f'{prefix}_all_steps_{filename.name}.xlsx'

            df_right = iforest_prediction(filename,
                                          physical_quantity,
                                          is_all_step,
                                          model)

            if not df_right.empty:
                df_right.drop(columns='Label', inplace=True)
                df_right.rename(columns={'first_step': f'{filename.name}_first_step',
                                         'last_step': f'{filename.name}_last_step',
                                         'Score': f'{filename.name}_score'},
                                inplace=True)
                columns = {col: f'{filename.name}_{col}'
                           for col in df_right.columns.values.tolist()
                           if col == 'middle_steps'}

                df_right.rename(columns=columns, inplace=True)

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


def main():
    save_prediction_to_exel('all', 'isotope',
                            is_all_step=False,
                            model_name='nuc_isotope_model',
                            merge=True)


main()
