from pathlib import Path

import pandas as pd
from sqlalchemy import select, or_, insert, lambda_stmt

from db.base import Session
from db.db_model import File, Nuc, NucData, ExtractedData, PhysicalQuantity
from db.fetch_data import fetch_physical_quantities_by_name, fetch_files_by_name
from utils.formatter import type_checker
from utils.middle_steps import middle_steps_line_parsing
from utils.workbook import append_df_to_excel


def save_extracted_data_to_db(filenames=None, physical_quantities='all', nuclide_list=None):
    """
    将数据存入到ExtractedData表里

    Parameters
    ----------
    filenames : list[File] or File
        File object
    physical_quantities : list[str] or str or list[PhysicalQuantity] or PhysicalQuantity
        物理量，可以是物理量名的list[str]或str，
        也可以是list[PhysicalQuantity]或PhysicalQuantity
    nuclide_list : list[str]
        核素list
    """

    if type_checker(filenames, File) == 'str':
        filenames = fetch_files_by_name(filenames)

    if type_checker(physical_quantities, PhysicalQuantity) == 'str':
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

                        gamma_stmt = (select(NucData.nuc_id, NucData.file_id, NucData.physical_quantity_id,
                                             NucData.last_step, NucData.middle_steps).
                                      where(NucData.file_id == file_id,
                                            NucData.physical_quantity_id == gamma_physical_quantity_id)
                                      )

                        gamma_insert_stmt = insert(ExtractedData).from_select(
                            names=['nuc_id',
                                   'file_id',
                                   'physical_quantity_id',
                                   'last_step',
                                   'middle_steps'],
                            select=gamma_stmt)
                        session.execute(gamma_insert_stmt)

                stmt = (select(NucData.nuc_id, NucData.file_id, NucData.physical_quantity_id,
                               NucData.last_step, NucData.middle_steps).
                        join(Nuc, Nuc.id == NucData.nuc_id).
                        where(NucData.file_id == file_id,
                              NucData.physical_quantity_id.in_(physical_quantities_id)).
                        where(Nuc.name.in_(nuclide_list))
                        )

            # 用INSERT INTO FROM SELECT将数据插入ExtractedData table
            insert_stmt = insert(ExtractedData).from_select(
                            names=['nuc_id',
                                   'file_id',
                                   'physical_quantity_id',
                                   'last_step',
                                   'middle_steps'],
                            select=stmt)
            session.execute(insert_stmt)

            session.commit()


def save_extracted_data_to_exel(filenames=None, is_all_step=False, dir_path=Path('.'), merge=True):
    """
    将数据存入到exel文件
    将传入的File list中包含的文件的数据存到exel文件
    如无filenames is None，则包含所有文件
    
    Parameters
    ----------
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

    if merge:
        file_path = dir_path.joinpath('final.xlsx')
        file_path.unlink(missing_ok=True)

    with Session() as session:
        physical_quantity: PhysicalQuantity
        for physical_quantity in physical_quantities:
            df_left = pd.DataFrame(data=None, columns=['nuc_ix', 'name'])

            physical_quantity_id = physical_quantity.id
            filename: File
            for filename in filenames:
                file_id = filename.id

                if not merge:
                    file_path = dir_path.joinpath(f'{filename.name}.xlsx')
                    file_path.unlink(missing_ok=True)

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


def main():
    filenames = fetch_files_by_name()


if __name__ == '__main__':
    main()
