from decimal import Decimal

import pandas as pd

from db.db_model import PhysicalQuantity, File
from db.fetch_data import fetch_extracted_data_by_filename_and_physical_quantity, \
    fetch_all_filenames, fetch_physical_quantities_by_name
from utils.physical_quantity_list_generator import is_it_all_str


def _complement_columns(df_reference, df_comparison,
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


def calculate_comparative_result(reference_file,
                                 comparison_files,
                                 physical_quantities='isotope',
                                 deviation_mode='relative',
                                 threshold=Decimal(1.0E-12),
                                 is_all_step=False):
    """
    选定一个基准文件，与其他文件进行对比，计算对比结果

    Parameters
    ----------
    reference_file : File
        基准文件
    comparison_files : list[File] or File
        对比文件列表
    physical_quantities : list[str] or str or list[PhysicalQuantity] or PhysicalQuantity, default = 'isotope'
        对比用物理量，可以是物理量名的list[str]或str，
        也可以是PhysicalQuantity list也可以是list[PhysicalQuantity]或PhysicalQuantity
        默认为核素密度
    deviation_mode : str, default = 'relative'
        绝对=absolute
        相对=relative
        偏差模式，分为绝对和相对，默认为相对
    threshold : Decimal, default = Decimal(1.0E-12)
        偏差阈值，默认1.0E-12
    is_all_step : bool, default = False
        是否读取全部中间结果数据列，默认只读取最终结果列
    Returns
    -------

    """
    if is_it_all_str(physical_quantities):
        physical_quantities = fetch_physical_quantities_by_name(physical_quantities)

    comparison_file: File
    for comparison_file in comparison_files:
        reference_data = fetch_extracted_data_by_filename_and_physical_quantity(reference_file,
                                                                                physical_quantities,
                                                                                is_all_step)

        comparison_data = fetch_extracted_data_by_filename_and_physical_quantity(comparison_file,
                                                                                 physical_quantities,
                                                                                 is_all_step
                                                                                 )
        physical_quantity: PhysicalQuantity
        for physical_quantity in physical_quantities:
            reference_data[physical_quantity.name], \
                comparison_data[physical_quantity.name] = _complement_columns(reference_data[physical_quantity.name],
                                                                              comparison_data[physical_quantity.name],
                                                                              reference_file.name,
                                                                              comparison_file.name)



def main():
    filenames = fetch_all_filenames()
    calculate_comparative_result(filenames.pop(2),
                                 filenames,
                                 is_all_step=True)


if __name__ == '__main__':
    main()
