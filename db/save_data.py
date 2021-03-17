from decimal import Decimal
from pathlib import Path

import pandas as pd
from sqlalchemy import select, or_, insert, lambda_stmt

from db.base import Session
from db.db_model import File, Nuc, NucData, ExtractedData, PhysicalQuantity
from db.fetch_data import fetch_physical_quantities_by_name, fetch_all_filenames, \
    fetch_extracted_data_by_filename_and_physical_quantity
from utils.middle_steps import middle_steps_line_parsing
from utils.physical_quantity_list_generator import is_it_all_str
from utils.worksheet import append_df_to_excel


def save_extracted_data_to_db(filenames=None, physical_quantities='all', nuclide_list=None):
    """
    将数据存入到ExtractedData表里

    Parameters
    ----------
    filenames : list[File] or File
        File object
    physical_quantities : list[str] or str or list[PhysicalQuantity] or PhysicalQuantity
        物理量，可以是物理量名的list[str]或str，
        也可以是PhysicalQuantity list也可以是list[PhysicalQuantity]或PhysicalQuantity
    nuclide_list : list[str]
        核素list
    """

    if filenames is None:
        filenames = fetch_all_filenames()
    if not isinstance(filenames, list):
        filenames = [filenames]

    if is_it_all_str(physical_quantities):
        physical_quantities = fetch_physical_quantities_by_name(physical_quantities)

    with Session() as session:
        for filename in filenames:
            physical_quantities: list[PhysicalQuantity]
            physical_quantities_id = [physical_quantity.id
                                      for physical_quantity in physical_quantities]
            file_id = filename.id

            if nuclide_list is None:
                # 核素列表为空则过滤first_step和last_step皆为0的records
                stmt = (select(NucData.nuc_id, NucData.file_id, NucData.physical_quantity_id,
                               NucData.last_step, NucData.middle_steps).
                        where(NucData.file_id == file_id,
                              NucData.physical_quantity_id.in_(physical_quantities_id)).
                        where(or_(NucData.first_step != 0, NucData.last_step != 0))
                        )
            else:
                # 核素不为gamma时，依照核素列表过滤records，否则反之
                for physical_quantity in physical_quantities:
                    if physical_quantity.name == 'gamma_spectra':
                        gamma_physical_quantity_id = physical_quantity.id

                stmt = (select(NucData.nuc_id, NucData.file_id, NucData.physical_quantity_id,
                               NucData.last_step, NucData.middle_steps).
                        join(Nuc, Nuc.id == NucData.nuc_id).
                        where(NucData.file_id == file_id,
                              NucData.physical_quantity_id.in_(physical_quantities_id)).
                        where(Nuc.name.in_(nuclide_list))
                        )

                gamma_stmt = (select(NucData.nuc_id, NucData.file_id, NucData.physical_quantity_id,
                                     NucData.last_step, NucData.middle_steps).
                              where(NucData.file_id == file_id,
                                    NucData.physical_quantity_id == gamma_physical_quantity_id)
                              )

                gamma_insert_stmt = insert(ExtractedData).from_select(
                    names=['nuc_id', 'file_id', 'physical_quantity_id', 'last_step',
                           'middle_steps'],
                    select=gamma_stmt)
                session.execute(gamma_insert_stmt)

            # 用INSERT INTO FROM SELECT将数据插入ExtractedData table
            insert_stmt = insert(ExtractedData).from_select(
                names=['nuc_id', 'file_id', 'physical_quantity_id', 'last_step',
                       'middle_steps'],
                select=stmt)

            session.execute(insert_stmt)
            session.commit()


def save_extracted_data_to_exel(filenames=None, is_all_step=False, file_path=Path('.')):
    """
    将数据存入到exel文件
    将传入的File list中包含的文件的数据存到exel文件
    如无filenames is None，则包含所有文件
    Parameters
    ----------
    filenames : comparison_files : list[File] or File
    is_all_step : bool, default false
        是否读取全部中间结果数据列，默认只读取最终结果列
    file_path : Path
    Returns
    -------

    """
    if filenames is None:
        filenames = fetch_all_filenames()
    if not isinstance(filenames, list):
        filenames = [filenames]

    physical_quantities = fetch_physical_quantities_by_name('all')

    with Session() as session:
        physical_quantity: PhysicalQuantity
        for physical_quantity in physical_quantities:
            df_left = pd.DataFrame(data=None, columns=['nuc_ix', 'name'])

            physical_quantity_id = physical_quantity.id
            filename: File
            for filename in filenames:
                file_id = filename.id

                if not is_all_step:
                    # 不读取中间结果，所以不选择NucData.middle_steps，否则反之
                    stmt = lambda_stmt(lambda: select(Nuc.nuc_ix, Nuc.name,
                                                      ExtractedData.last_step))
                else:
                    stmt = lambda_stmt(lambda: select(Nuc.nuc_ix, Nuc.name,
                                                      ExtractedData.last_step,
                                                      ExtractedData.middle_steps))

                stmt += lambda s: s.join(Nuc,
                                         Nuc.id == ExtractedData.nuc_id)
                stmt += lambda s: s.join(PhysicalQuantity,
                                         PhysicalQuantity.id == ExtractedData.physical_quantity_id)
                stmt += lambda s: s.where(ExtractedData.file_id == file_id,
                                          PhysicalQuantity.id == physical_quantity_id)

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
                                                for name in middle_steps.columns.values.tolist()]
                    middle_steps.columns = middle_step_column_names

                    df_right = pd.concat([exclude_middle_steps, middle_steps], axis=1, copy=False)

                if not df_right.empty:
                    df_left = pd.merge(df_left, df_right, how='outer', on=['nuc_ix', 'name'])

            append_df_to_excel(file_path, df_left, sheet_name=physical_quantity.name, index=False)


def save_comparative_result_to_excel(reference_file,
                                     comparison_files,
                                     physical_quantities='isotope',
                                     deviation_mode='relative',
                                     threshold=Decimal(1.0E-12),
                                     is_all_step=False):
    """
    选定一个基准文件，与其他文件进行对比，将结果保存至xlsx文件

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
            reference_data_column_length = len(reference_data[physical_quantity.name].columns)
            comparison_data_column_length = len(comparison_data[physical_quantity.name].columns)
            column_length_difference = reference_data_column_length - comparison_data_column_length

            column_start_num = min(reference_data_column_length, comparison_data_column_length) - 2

            if reference_data_column_length < comparison_data_column_length:
                complement_columns = [f'{reference_file.name}_middle_steps_{i}' for i in
                                      range(column_start_num, column_start_num+abs(column_length_difference))]
                complement_df = pd.DataFrame(data=None, columns=complement_columns)
                reference_data[physical_quantity.name] = pd.concat([reference_data[physical_quantity.name],
                                                                    complement_df], axis=1, copy=False)
            else:
                complement_columns = [f'{comparison_file.name}_middle_steps_{i}' for i in
                                      range(column_start_num, column_start_num + abs(column_length_difference))]
                complement_df = pd.DataFrame(data=None, columns=complement_columns)
                comparison_data[physical_quantity.name] = pd.concat([comparison_data[physical_quantity.name],
                                                                     complement_df], axis=1, copy=False)

            

            print()



def main():
    filenames = fetch_all_filenames()
    save_comparative_result_to_excel(filenames.pop(2),
                                     filenames,
                                     is_all_step=True)


if __name__ == '__main__':
    main()
