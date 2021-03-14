from utils.configlib import Config


def _get_physical_quantity_list_by_conf_file():
    """
    从配置文件获取全部物理量名
    Returns
    -------
    list
        全部物理量
    """
    return [key for key in Config.get_data_extraction_conf('keys_of_rows') if key != 'all']


# 从配置文件获取全部物理量名
all_physical_quantity_list = _get_physical_quantity_list_by_conf_file()


def physical_quantity_list_generator(physical_quantity_name):
    """
    根据输入生成对应的physical_quantity list,如果输入为all，返回全部物理量list

    Parameters
    ----------
    physical_quantity_name : str or list

    Returns
    -------
    list
    """
    if isinstance(physical_quantity_name, str):
        """如果输入的是str,则将其包装为list"""
        physical_quantity_name = [physical_quantity_name]

    if not all(physical_quantity in all_physical_quantity_list for physical_quantity in set(physical_quantity_name)) \
            and physical_quantity_name != 'all':
        """
        遍历输入(physical_quantity_name)，如果不是每个element都在all_physical_quantity_list内
        并且输入不是all，设置输入为all
        """
        physical_quantity_name = 'all'

    # 如果输入为all，返回全部物理量list，否，
    # 则先用set()去掉重复物理量名，然后再return 物理量list
    return all_physical_quantity_list if physical_quantity_name == 'all' else list(set(physical_quantity_name))


def is_it_all_str(str_or_list):
    """
    判断输入的是不是str或str of list
    Parameters
    ----------
    str_or_list : 输入
    Returns
    -------
    bool
    """
    is_in = False
    if isinstance(str_or_list, str) or \
            all(isinstance(e, str)
                for e in str_or_list):
        is_in = True
    return is_in
