from pathlib import Path

import pandas as pd
from pycaret.anomaly import setup, create_model, predict_model, load_model

from nuc_data_tool.db.db_model import File, PhysicalQuantity
from nuc_data_tool.db.fetch_data import (fetch_files_by_name,
                                         fetch_physical_quantities_by_name,
                                         fetch_data_by_filename_and_physical_quantity)
from nuc_data_tool.utils.formatter import type_checker
from nuc_data_tool.utils.workbook import save_to_excel


def _complement_columns(nuc_data,
                        middle_steps_num):
    """
    补全 middle_step column 以便用于模型预测

    Parameters
    ----------
    nuc_data : pd.DataFrame
    middle_steps_num : int

    Returns
    -------
    pd.DataFrame
    """

    nuc_data_middle_steps_column_length = len(nuc_data.columns) - 4

    if middle_steps_num > nuc_data_middle_steps_column_length:
        complement_columns = [f'middle_step_{i}'
                              for i in range(nuc_data_middle_steps_column_length + 1,
                                             middle_steps_num + 1)]
        complement_df = pd.DataFrame(data=None, columns=complement_columns)
        nuc_data = pd.concat([nuc_data, complement_df], axis=1, copy=False)

    return nuc_data


def train_model(nuc_data,
                model_type,
                fraction):
    """

    Parameters
    ----------
    nuc_data : pd.DataFrame
    model_type : str
    fraction : float
    Returns
    -------

    """
    unnecessary_columns = ['nuc_ix', 'name']
    numeric_columns = [col for col in nuc_data.columns.tolist()
                       if col not in unnecessary_columns]

    exp_ano = setup(nuc_data,
                    normalize=True,
                    normalize_method='robust',
                    ignore_features=unnecessary_columns,
                    numeric_features=numeric_columns,
                    silent=True)

    kargs = {}
    if fraction is not None:
        kargs['fraction'] = fraction

    model = create_model(model_type, **kargs)

    return model


def prediction(filenames,
               physical_quantity='isotope',
               is_all_step=False,
               model_type='iforest',
               model=None,
               fraction=0.01):
    """

    Parameters
    ----------
    filenames : list[File or str] or File or str
    physical_quantity : str or PhysicalQuantity, default = 'isotope'
        物理量，可以是物理量名的list[str]或str，
        默认为核素密度
    is_all_step : bool, default = False
        是否读取全部中间结果数据列，默认只读取最终结果列
    model_type : str
    model
    fraction : float

    Returns
    -------
    pd.DataFrame
    """

    if type_checker(filenames, File) == 'str':
        filenames = fetch_files_by_name(filenames)

    if type_checker(physical_quantity, PhysicalQuantity) == 'str':
        physical_quantity = fetch_physical_quantities_by_name(physical_quantity).pop()

    nuc_data_left = pd.DataFrame(columns=['nuc_ix', 'name'])

    for filename in filenames:
        nuc_data_right = fetch_data_by_filename_and_physical_quantity(filename, physical_quantity, is_all_step)

        if nuc_data_right.empty:
            continue

        nuc_data_right.rename(columns={'first_step': f'{filename.name}_first_step',
                                       'last_step': f'{filename.name}_last_step'},
                              inplace=True)
        columns = {col: f'{filename.name}_{col}'
                   for col in nuc_data_right.columns.tolist()
                   if 'middle_step' in col}
        nuc_data_right.rename(columns=columns, inplace=True)

        numeric_columns = [col for col in nuc_data_right.columns.tolist()
                           if col not in ['nuc_ix', 'name']]
        nuc_data_right[numeric_columns] = nuc_data_right[numeric_columns].astype('float64', copy=False)

        nuc_data_left = pd.merge(nuc_data_left, nuc_data_right, how='outer', on=['nuc_ix', 'name'])

    if model_type is not None:
        model = train_model(nuc_data=nuc_data_left, model_type=model_type, fraction=fraction)

    result_prediction = predict_model(model, data=nuc_data_left)

    return result_prediction[result_prediction['Anomaly'] == 1].drop(columns='Anomaly')


def save_prediction_to_exel(filenames,
                            result_path,
                            physical_quantities='isotope',
                            is_all_step=False,
                            merge=True,
                            model_type=None,
                            model_name=None,
                            fraction=0.001):
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
    model_type : str
    model_name : str
    fraction
    Returns
    -------

    """

    if type_checker(filenames, File) == 'str':
        filenames = fetch_files_by_name(filenames)

    if type_checker(physical_quantities, PhysicalQuantity) == 'str':
        physical_quantities = fetch_physical_quantities_by_name(physical_quantities)

    if model_type is None:
        model = load_model(model_name)
    else:
        model = None

    result_path = Path(result_path).joinpath('anomaly_detection_result')

    prefix = model_type

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

        if merge:
            df_result = prediction(filenames=filenames,
                                   physical_quantity=physical_quantity,
                                   is_all_step=is_all_step,
                                   model_type=model_type,
                                   model=model,
                                   fraction=fraction)

            df_result.dropna(axis=1, how='all', inplace=True)
            save_to_excel({physical_quantity.name: df_result},
                          file_name,
                          result_path)
        else:
            df_left = pd.DataFrame(data=None, columns=['nuc_ix', 'name'])

            for filename in filenames:

                files_name = f'{prefix}_{filename.name}.xlsx'
                if is_all_step:
                    files_name = f'{prefix}_all_steps_{filename.name}.xlsx'

                df_right = prediction(filenames=filenames,
                                      physical_quantity=physical_quantity,
                                      is_all_step=is_all_step,
                                      model_type=model_type,
                                      model=model,
                                      fraction=fraction)

                if not df_right.empty:
                    df_right.rename(columns={'Anomaly_Score': f'{filename.name}_Anomaly_Score'},
                                    inplace=True)

                    df_right.dropna(axis=1, how='all', inplace=True)
                    df_left = pd.merge(df_left, df_right, how='outer', on=['nuc_ix', 'name'])

                save_to_excel({physical_quantity.name: df_left},
                              files_name,
                              result_path)
                df_left = pd.DataFrame(data=None, columns=['nuc_ix', 'name'])


# def main():
#     """"""
#     from nuc_data_tool.db.fetch_data import fetch_files_by_name
#     files = fetch_files_by_name()
#
#     print(prediction(filenames=files,
#                      is_all_step=True,
#                      model_type='lof',
#                      fraction=0.01
#                      ))
#
#
# main()
