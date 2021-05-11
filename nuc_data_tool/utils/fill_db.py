import pandas as pd
from sqlalchemy import select

from nuc_data_tool.db.base import Session
from nuc_data_tool.db.db_model import Nuc, NucData, File, PhysicalQuantity
from nuc_data_tool.db.db_utils import upsert
from nuc_data_tool.utils.middle_steps import middle_steps_line_serialization


def populate_database(xml_file):
    """
    将xml_file的数据填入数据库

    Parameters
    ----------
    xml_file: InputXmlFileReader

    Returns
    -------

    """
    session = Session()

    # 依据文件名获取对应的File object
    file_stmt = (select(File)
                 .where(File.name == xml_file.name)
                 )
    file_tmp = session.execute(file_stmt).scalar_one_or_none()
    if file_tmp is None:
        # 如果数据库不存在对应的File records则插入
        file_tmp = File(name=xml_file.name)
        session.add(file_tmp)
    else:
        session.close()
        return

    for key in xml_file.table_of_physical_quantity:

        if not xml_file.table_of_physical_quantity[key]:
            # 为空则跳过
            continue

        # 依据物理量名获取对应的PhysicalQuantity object
        physical_quantity_stmt = (select(PhysicalQuantity)
                                  .where(PhysicalQuantity.name == key)
                                  )
        physical_quantity_tmp = session.execute(physical_quantity_stmt).scalar_one_or_none()
        if physical_quantity_tmp is None:
            # 如果数据库不存在对应的PhysicalQuantity records则插入
            physical_quantity_tmp = PhysicalQuantity(name=key)
            session.add(physical_quantity_tmp)

        # 关系插入
        file_tmp.physical_quantities.append(physical_quantity_tmp)
        physical_quantity_tmp.files.append(file_tmp)

        # 将序列化的middle_steps_line和其他数据存入DataFrame
        # 如核素为gamma,则从0依次赋予nuc_ix
        df_all_tmp = pd.DataFrame(middle_steps_line_serialization(data.split())
                                  if key != 'gamma_spectra'
                                  else middle_steps_line_serialization([i, *data.split()])
                                  for i, data in enumerate(xml_file.table_of_physical_quantity[key])
                                  )

        # 截取核素部分(nuc_ix和name)
        df_nuc_tmp: pd.DataFrame = df_all_tmp.iloc[:, [0, 1]]
        df_nuc_tmp.columns = ('nuc_ix', 'name')

        # upsert into db
        stmt = upsert(Nuc,
                      df_nuc_tmp.to_dict(orient='records'),
                      update_field=df_nuc_tmp.columns.tolist(),
                      engine=session.bind)

        session.execute(stmt)

        # 如果len(df_all_tmp.columns)为5则说明有middle_steps列
        # 将数据部分截取出来
        if len(df_all_tmp.columns) != 5:
            df_data_tmp: pd.DataFrame = df_all_tmp.iloc[:, [-2, -1]]
            df_data_tmp.columns = ('first_step', 'last_step')
        else:
            df_data_tmp: pd.DataFrame = df_all_tmp.iloc[:, -3:]
            df_data_tmp.columns = ('first_step', 'last_step', 'middle_steps')

        list_nuc_id = session.execute(select(Nuc.id).
                                      where(Nuc.nuc_ix.in_(df_nuc_tmp['nuc_ix']))).scalars().all()

        # 为数据部分生成3个外键
        df_data_prefix = pd.DataFrame({'nuc_id': list_nuc_id,
                                       'file_id': file_tmp.id,
                                       'physical_quantity_id': physical_quantity_tmp.id})
        # 合并外键和数据部分
        df_data_all = pd.concat([df_data_prefix, df_data_tmp],
                                axis=1, copy=False)

        # almost twice as slow as __table__.insert
        # session.execute(insert(NucData).values(df_data_all.to_dict(orient='records')))
        session.execute(NucData.__table__.insert(), df_data_all.to_dict(orient='records'))

        session.commit()

    session.close()
