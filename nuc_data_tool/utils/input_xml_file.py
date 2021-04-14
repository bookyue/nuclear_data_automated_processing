import linecache
from pathlib import Path

from nuc_data_tool.utils.configlib import config
from nuc_data_tool.utils.formatter import physical_quantity_list_generator


class InputXmlFileReader:
    """
    表示xml.out文件的类

    Attributes
    ----------
    path : Path
        xml.out文件路径
    physical_quantities : str
            核素名

    """
    chosen_physical_quantity: list
    length_of_physical_quantity: dict

    def __init__(self, file_path, physical_quantities='all'):
        """
        可以根据输入的文件路径和物理量
        自动计算得出选择的物理量chosen_physical_quantity和
        物理量对应的行号范围length_of_physical_quantity

        Parameters
        ----------
        file_path : Path or str
            xml.out文件路径
        physical_quantities : str or list[str]
            核素名
        """
        self.path = Path(file_path)
        self.name = Path(file_path).name.split('.')[0]
        self.chosen_physical_quantity = physical_quantity_list_generator(physical_quantities)
        self.length_of_physical_quantity = self.get_length_of_physical_quantity(self.chosen_physical_quantity)
        self.unfetched_physical_quantity = self.get_unfetched_physical_quantity()
        self.fetched_physical_quantity = self.get_fetched_physical_quantity()
        self.table_of_physical_quantity = self.get_table_of_physical_quantity()

    def __enter__(self):
        """
        with statement
        Returns
        -------
        InputXmlFileReader
        """
        self.file_object = self.path.open(mode='r', encoding='UTF-8')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file_object.close()

    def __getitem__(self, physical_quantity_name):
        tmp_dict = self.table_of_physical_quantity
        if physical_quantity_name != 'all':
            return {physical_quantity_name: tmp_dict.get(physical_quantity_name)}
        else:
            return self.table_of_physical_quantity

    def set_chosen_physical_quantity(self, physical_quantity_name):
        """
        输入选择的物理量，设置类的属性chosen_physical_quantity和length_of_physical_quantity
        Parameters
        ----------
        physical_quantity_name : str
            物理量名
        Returns
        -------

        """
        self.chosen_physical_quantity = physical_quantity_list_generator(physical_quantity_name)
        self.length_of_physical_quantity = self.get_length_of_physical_quantity()

    def get_length_of_physical_quantity(self, physical_quantities=None):
        """
        获取对应物理量的行号范围，physical_quantity_name默认为None,设置则输出physical_quantity_name对应的行号的字典
        但是此只是临时返回一个物理量及行号范围的字典，不修改类的属性self.chosen_physical_quantity和self.length_of_physical_quantity
        Parameters
        ----------
        physical_quantities: str or list[str]
            物理量名

        Returns
        -------
        dict[str, list[int]]
        """

        physical_quantities = physical_quantity_list_generator(physical_quantities)

        # 开始搜索关键字
        index_start = {physical_quantity: config.get_data_extraction_conf("keys_of_rows").get(physical_quantity)[0]
                       for physical_quantity in physical_quantities}
        # 结尾搜索关键字
        index_end = {physical_quantity: config.get_data_extraction_conf("keys_of_rows").get(physical_quantity)[-1]
                     for physical_quantity in physical_quantities}

        length_of_physical_quantity = {key: [] for key in physical_quantities}

        is_find_start_title = True

        for physical_quantity in physical_quantities:
            for row_number, line in enumerate(self.path.open(encoding='UTF-8')):
                if is_find_start_title:
                    if index_start[physical_quantity] in line:
                        length_of_physical_quantity[physical_quantity].append(
                            row_number + 7
                            if physical_quantity != 'gamma_spectra'
                            else
                            row_number + 2)
                        is_find_start_title = False
                else:
                    if index_end[physical_quantity] in line:
                        length_of_physical_quantity[physical_quantity].append(
                            row_number - 3
                            if physical_quantity != 'gamma_spectra'
                            else
                            row_number - 2)
                        is_find_start_title = True

        return length_of_physical_quantity

    def get_unfetched_physical_quantity(self):
        """
        获取在选取范围内却未能成功获取的物理量

        Returns
        -------
        list
        """
        unfetched_physical_quantity = [name for name in self.chosen_physical_quantity
                                       if not self.length_of_physical_quantity.get(name)]
        return unfetched_physical_quantity

    def get_fetched_physical_quantity(self):
        """
        获取在选取范围内成功获取的物理量

        Returns
        -------
        list
        """
        fetched_physical_quantity = [name for name in self.chosen_physical_quantity
                                       if self.length_of_physical_quantity.get(name)]
        return fetched_physical_quantity

    def get_table_of_physical_quantity(self):
        """
        依据选择的物理量和未能成功获取的物理量，以及对应物理量的行号范围
        从xml.out文件中使用linecache.getlines获取文本内容
        Returns
        -------
        dict[str, list[str]]
        """
        df_data = {}
        for key in self.chosen_physical_quantity:
            text = []
            if key not in self.unfetched_physical_quantity:
                row_start = self.length_of_physical_quantity[key][0]
                row_end = self.length_of_physical_quantity[key][1]
                text = linecache.getlines(str(self.path))[row_start:row_end + 1]

            df_data[key] = text
        return df_data
