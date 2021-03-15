import pandas as pd
from sqlalchemy import select, or_, insert, lambda_stmt

from db.base import Session
from db.db_model import File, Nuc, NucData, ExtractedData, PhysicalQuantity
from db.fetch_data import fetch_physical_quantities_by_name, fetch_all_filenames
from utils.configlib import Config
from utils.middle_steps import middle_steps_line_parsing
from utils.physical_quantity_list_generator import is_it_all_str
from utils.worksheet import append_df_to_excel


def save_extracted_data_to_db(filename, physical_quantities, nuclide_list):
    """
    将数据存入到ExtractedData表里

    Parameters
    ----------
    filename : File
        File object
    physical_quantities: list or str
        核素名，可以是核素名的list或str，也可以是PhysicalQuantity list
    nuclide_list : list
        核素list
    """
    with Session() as session:
        if is_it_all_str(physical_quantities):
            physical_quantities = fetch_physical_quantities_by_name(physical_quantities)

        physical_quantity: PhysicalQuantity
        for physical_quantity in physical_quantities:
            file_id = filename.id
            physical_quantity_id = physical_quantity.id

            if nuclide_list is None:
                """核素列表为空则过滤first_step和last_step皆为0的records"""
                stmt = (select(NucData.nuc_id, NucData.file_id, NucData.physical_quantity_id, NucData.last_step,
                               NucData.middle_steps).
                        where(NucData.file_id == file_id, NucData.physical_quantity_id == physical_quantity_id).
                        where(or_(NucData.first_step != 0, NucData.last_step != 0))
                        )
            else:
                if physical_quantity.name != 'gamma_spectra':
                    """核素不为gamma时，依照核素列表过滤records，否则反之"""
                    stmt = (select(NucData.nuc_id, NucData.file_id, NucData.physical_quantity_id, NucData.last_step,
                                   NucData.middle_steps).
                            join(Nuc, Nuc.id == NucData.nuc_id).
                            where(NucData.file_id == file_id, NucData.physical_quantity_id == physical_quantity_id).
                            where(Nuc.name.in_(nuclide_list))
                            )
                else:
                    stmt = (select(NucData.nuc_id, NucData.file_id, NucData.physical_quantity_id, NucData.last_step,
                                   NucData.middle_steps).
                            where(NucData.file_id == file_id, NucData.physical_quantity_id == physical_quantity_id)
                            )
            # 用INSERT INTO FROM SELECT将数据插入ExtractedData table
            insert_stmt = insert(ExtractedData).from_select(
                names=['nuc_id', 'file_id', 'physical_quantity_id', 'last_step',
                       'middle_steps'],
                select=stmt)

            session.execute(insert_stmt)
            session.commit()


def save_save_extracted_data_to_exel(filenames=None, is_all_step=False):
    """
    将数据存入到exel文件
    将传入的File list中包含的文件的数据存到exel文件
    如无filenames is None，则包含所有文件
    Parameters
    ----------
    filenames : list or File
    is_all_step : bool, default false
        是否读取全部中间结果数据列，默认只读取最终结果列

    Returns
    -------

    """
    if filenames is None:
        filenames = fetch_all_filenames()
    if not isinstance(filenames, list):
        filenames = [filenames]

    physical_quantities = fetch_physical_quantities_by_name('all')
    file_path = Config.get_file_path('final_file_path')
    with Session() as session:
        physical_quantity: PhysicalQuantity
        for physical_quantity in physical_quantities:
            filename: File
            df_left = pd.DataFrame(data=None, columns=['nuc_ix', 'name'])
            for filename in filenames:
                file_id = filename.id
                physical_quantity_id = physical_quantity.id

                if not is_all_step:
                    """不读取中间结果，所以不选择NucData.middle_steps，否则反之"""
                    stmt = lambda_stmt(lambda: select(Nuc.nuc_ix, Nuc.name, ExtractedData.last_step))
                else:
                    stmt = lambda_stmt(lambda: select(Nuc.nuc_ix, Nuc.name,
                                                      ExtractedData.last_step, ExtractedData.middle_steps))

                stmt += lambda s: s.join(Nuc, Nuc.id == ExtractedData.nuc_id)
                stmt += lambda s: s.join(PhysicalQuantity, PhysicalQuantity.id == ExtractedData.physical_quantity_id)
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
                    df_left = pd.merge(df_left, df_right, how='outer', on=['nuc_ix', 'name'], sort=False)

            append_df_to_excel(file_path, df_left, sheet_name=physical_quantity.name, index=False)


def main():
    filenames = fetch_all_filenames()

    save_save_extracted_data_to_exel(filenames, True)


if __name__ == '__main__':
    main()
