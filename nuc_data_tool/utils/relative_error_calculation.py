from decimal import localcontext, Decimal, InvalidOperation
from pathlib import Path

import numpy as np
import pandas as pd

from nuc_data_tool.db.db_model import PhysicalQuantity, File
from nuc_data_tool.db.fetch_data import (fetch_extracted_data_by_filename_and_physical_quantity,
                                         fetch_files_by_name,
                                         fetch_physical_quantities_by_name)
from nuc_data_tool.utils.formatter import type_checker
from nuc_data_tool.utils.workbook import save_to_excel


def _complement_columns(df_reference,
                        df_comparison,
                        reference_complement_column_name,
                        comparison_complement_column_name):
    """
    对齐middle_step_* columns，数值填充为NaN

    Parameters
    ----------
    df_reference : pd.DataFrame
    df_comparison : pd.DataFrame
    reference_complement_column_name : str
    comparison_complement_column_name : str

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
    """
    reference_column_length = len(df_reference.columns)
    comparison_column_length = len(df_comparison.columns)
    column_length_difference = reference_column_length - comparison_column_length

    column_start_num = min(reference_column_length, comparison_column_length) - 2

    if reference_column_length == comparison_column_length:
        # 当column length相同时，什么也不做
        pass
    elif reference_column_length < comparison_column_length:
        complement_columns = [f'{reference_complement_column_name}_middle_step_{i}'
                              for i in range(column_start_num,
                                             column_start_num + abs(column_length_difference))]
        complement_df = pd.DataFrame(data=None, columns=complement_columns)
        df_reference = pd.concat([df_reference, complement_df], axis=1, copy=False)
    else:
        complement_columns = [f'{comparison_complement_column_name}_middle_step_{i}'
                              for i in range(column_start_num,
                                             column_start_num + abs(column_length_difference))]
        complement_df = pd.DataFrame(data=None, columns=complement_columns)
        df_comparison = pd.concat([df_comparison, complement_df], axis=1, copy=False)

    return df_reference, df_comparison


def _calculate_deviation(df_reference,
                         df_comparison,
                         deviation_mode='relative',
                         threshold=Decimal('1.0E-12')):
    """
    计算误差
    relative deviation formula: abs(X - Y) / 1 + min(abs(X), abs(Y))
    absolute deviation formula: X - Y

    Parameters
    ----------
    df_reference : pd.DataFrame
    df_comparison : pd.DataFrame
    deviation_mode : str, default = 'relative'
        绝对=absolute
        相对=relative
        偏差模式，分为绝对和相对，默认为相对

    Returns
    -------
    tuple[pd.DataFrame, pd.Series]
    """

    df_deviation = pd.DataFrame()
    for reference_column, \
        comparison_column in zip(df_reference.columns.values.tolist()[2:],
                                 df_comparison.columns.values.tolist()[2:]):
        i = 0
        if deviation_mode == 'relative':
            # abs(X - Y) / 1 + min(abs(X), abs(Y))
            """NaN is classified as unordered.
            By default InvalidOperation is trapped, 
            so a Python exception is raised when using <= and >= against Decimal('NaN'). 
            This is a logical extension; 
            Python has actual exceptions so if you compare against the 
            NaN exception value, you can expect an exception being raised. 
            You could disable trapping by using a Decimal.localcontext()
            https://stackoverflow.com/a/28371465/11071374
            """
            with localcontext() as ctx:
                ctx.traps[InvalidOperation] = False
                deviation = (df_reference[reference_column] - df_comparison[comparison_column]).abs() / \
                            (1 + np.minimum(df_reference[reference_column].fillna(Decimal('NaN')),
                                            df_comparison[comparison_column].fillna(Decimal('NaN'))))

        elif deviation_mode == 'absolute':
            deviation = (df_reference[reference_column] - df_comparison[comparison_column]).abs()

        else:
            raise Exception("wrong deviation mode")

        if not deviation.isnull().values.all():
            df_deviation[f'relative_deviation_middle_step_{i}'] = deviation

        i += 1

    df_deviation.rename(columns={'relative_deviation_middle_step_0': 'relative_deviation_last_step'}, inplace=True)

    reserved_index = (df_deviation.T > threshold).any()
    df_deviation = df_deviation.loc[reserved_index].reset_index(drop=True)

    return df_deviation, reserved_index


def _merge_reference_comparison_and_deviation(df_reference,
                                              df_comparison,
                                              df_deviation,
                                              reserved_index,
                                              reserved_na=False):
    """
    依据reserved_index合并reference，comparison和deviation表
    如果reserved_na为False,则drop columns with all NaN's，否则反之
    默认reserved_na为False

    Parameters
    ----------
    df_reference : pd.DataFrame
    df_comparison : pd.DataFrame
    df_deviation : pd.DataFrame
    reserved_index : pd.Series
    reserved_na : bool, default = False
        如果reserved_na为False,则drop columns with all NaN's，否则反之

    Returns
    -------
    pd.DataFrame
    """
    df_ = pd.merge(df_reference.loc[reserved_index],
                   df_comparison.loc[reserved_index],
                   how='outer', on=['nuc_ix', 'name'])

    df_all = pd.concat([df_, df_deviation], axis=1, copy=False)

    if not reserved_na:
        df_all.dropna(axis=1, how='all', inplace=True)

    return df_all


def calculate_comparative_result(nuc_data_id,
                                 reference_file,
                                 comparison_file,
                                 physical_quantities='isotope',
                                 deviation_mode='relative',
                                 threshold=Decimal('1.0E-12'),
                                 is_all_step=False):
    """
    选定一个基准文件，一个对比文件，与其进行对比，计算并返回对比结果

    Parameters
    ----------
    nuc_data_id : list[int]
    reference_file : File or str
        基准文件
    comparison_file : File or str
        对比文件
    physical_quantities : list[str or PhysicalQuantity] or str or PhysicalQuantity, default = 'isotope'
        对比用物理量，可以是物理量名的list[str]或str，
        也可以是PhysicalQuantity list也可以是list[PhysicalQuantity]或PhysicalQuantity
        默认为核素密度
    deviation_mode : str, default = 'relative'
        绝对=absolute
        相对=relative
        偏差模式，分为绝对和相对，默认为相对
    threshold : Decimal, default = Decimal('1.0E-12')
        偏差阈值，默认1.0E-12
    is_all_step : bool, default = False
        是否读取全部中间结果数据列，默认只读取最终结果列

    Returns
    -------
    dict[str, pd.DataFrame]
    """

    if type_checker([reference_file, comparison_file], File) == 'str':
        reference_file = fetch_files_by_name(reference_file).pop()
        comparison_file = fetch_files_by_name(comparison_file).pop()

    if type_checker(physical_quantities, PhysicalQuantity) == 'str':
        physical_quantities = fetch_physical_quantities_by_name(physical_quantities)

    dict_df_all = {}

    physical_quantity: PhysicalQuantity
    for physical_quantity in physical_quantities:
        reference_data = fetch_extracted_data_by_filename_and_physical_quantity(nuc_data_id,
                                                                                reference_file,
                                                                                physical_quantity,
                                                                                is_all_step)

        comparison_data = fetch_extracted_data_by_filename_and_physical_quantity(nuc_data_id,
                                                                                 comparison_file,
                                                                                 physical_quantity,
                                                                                 is_all_step
                                                                                 )

        if reference_data.empty or comparison_data.empty:
            continue

        reference_data, comparison_data = _complement_columns(reference_data,
                                                              comparison_data,
                                                              reference_file.name,
                                                              comparison_file.name)

        df_deviation, reserved_index = _calculate_deviation(reference_data,
                                                            comparison_data,
                                                            deviation_mode,
                                                            Decimal(threshold))

        dict_df_all[physical_quantity.name] = _merge_reference_comparison_and_deviation(reference_data,
                                                                                        comparison_data,
                                                                                        df_deviation,
                                                                                        reserved_index)

    return dict_df_all


def save_comparison_result_to_excel(nuc_data_id,
                                    reference_file,
                                    comparison_files,
                                    result_path,
                                    physical_quantities='isotope',
                                    deviation_mode='relative',
                                    threshold=Decimal('1.0E-12'),
                                    is_all_step=False):
    """
    选定一个基准文件，使其与对比文件列表中的文件一一对比，计算并输出对比结果至工作簿(xlsx文件)

    Parameters
    ----------
    nuc_data_id : list[int]
    reference_file : File or str
        基准文件
    comparison_files : list[str or File]or File or str
        对比文件列表
    result_path : Path or str
    physical_quantities : list[str or PhysicalQuantity] or str or PhysicalQuantity, default = 'isotope'
        对比用物理量，可以是物理量名的list[str]或str，
        也可以是PhysicalQuantity list也可以是list[PhysicalQuantity]或PhysicalQuantity
        默认为核素密度
    deviation_mode : str, default = 'relative'
        绝对=absolute
        相对=relative
        偏差模式，分为绝对和相对，默认为相对
    threshold : Decimal, default = Decimal('1.0E-12')
        偏差阈值，默认1.0E-12
    is_all_step : bool, default = False
        是否读取全部中间结果数据列，默认只读取最终结果列

    Returns
    -------
    """

    if type_checker(reference_file, File) == 'str':
        reference_file = fetch_files_by_name(reference_file)

    if type_checker(comparison_files, File) == 'str':
        comparison_files = fetch_files_by_name(comparison_files)

    for comparison_file in comparison_files:
        print((reference_file.name, comparison_file.name))

        dict_df_all = calculate_comparative_result(nuc_data_id=nuc_data_id,
                                                   reference_file=reference_file,
                                                   comparison_file=comparison_file,
                                                   physical_quantities=physical_quantities,
                                                   deviation_mode=deviation_mode,
                                                   threshold=threshold,
                                                   is_all_step=is_all_step)

        file_name = f'{deviation_mode}_{threshold}_{reference_file.name}_vs_{comparison_file.name}.xlsx'

        if is_all_step:
            file_name = f'all_step_{file_name}'

        Path(result_path).joinpath('comparative_result').joinpath(file_name).unlink(missing_ok=True)
        save_to_excel(dict_df_all,
                      file_name,
                      Path(result_path).joinpath('comparative_result'))
