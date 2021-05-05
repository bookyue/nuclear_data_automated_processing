from typing import Iterable

from nuc_data_tool.db.db_model import PhysicalQuantity, File
from nuc_data_tool.utils.configlib import config


def _get_physical_quantity_list_by_conf_file():
    """
    从配置文件获取全部物理量名
    Returns
    -------
    list
        全部物理量
    """
    return [key for key in config.get_data_extraction_conf('keys_of_rows')]


# 从配置文件获取全部物理量名
all_physical_quantity_list = _get_physical_quantity_list_by_conf_file()


def physical_quantity_list_generator(physical_quantities):
    """
    根据输入生成对应的physical_quantity list,如果输入为all，返回全部物理量list

    Parameters
    ----------
    physical_quantities : str or list[str]

    Returns
    -------
    list[str]
    """
    if isinstance(physical_quantities, str):
        # 如果输入的是str,则将其包装为list
        physical_quantities = [physical_quantities]

    if not all(physical_quantity in all_physical_quantity_list
               for physical_quantity in set(physical_quantities)) \
            and physical_quantities != 'all':

        # 遍历输入(physical_quantities)，如果不是每个element都在all_physical_quantity_list内
        # 并且输入不是all，设置输入为all

        physical_quantities = 'all'

    # 如果输入为all，返回全部物理量list，否，
    # 则先用set()去掉重复物理量名，然后再return 物理量list
    return all_physical_quantity_list \
        if physical_quantities == 'all' \
        else list(dict.fromkeys(physical_quantities).keys())


def type_checker(object_or_list, expected_type):
    """
    判断输入的是不是str or PhysicalQuantity or File or list[str or PhysicalQuantity or File]
    Parameters
    ----------
    object_or_list : str or PhysicalQuantity or File or list[str or PhysicalQuantity or File]
    expected_type : Any

    Returns
    -------
    str
    """

    if isinstance(object_or_list, Iterable):
        if all(isinstance(ele, str) for ele in object_or_list):
            return 'str'
        elif all(isinstance(ele, expected_type) for ele in object_or_list):
            return 'original'
        else:
            raise Exception('unexpected type')
    else:
        if isinstance(object_or_list, str):
            return 'str'
        elif isinstance(object_or_list, expected_type):
            return 'original'
        else:
            raise Exception('unexpected type')
