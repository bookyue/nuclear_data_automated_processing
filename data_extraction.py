import codecs
from decimal import Decimal

import pandas as pd
from cyberbrain import trace

from utils import configlib


@trace
def row_numbers_of_block(list_of_lines, keys_of_rows):
    """
    依据字符串，获取所在行号
    :param list_of_lines: 文件行
    :param keys_of_rows: 关键字
    :return: 关键字所在行号
    """

    is_all = False
    if len(keys_of_rows) > 2:
        is_all = True

    index_start = keys_of_rows[:-1]
    index_end = keys_of_rows[-1]

    is_gamma = False
    if index_start == ['Gamma-ray']:
        is_gamma = True

    row_numbers = []
    i = 0
    # Open the file in read only mode
    # Read all lines in the file one by one
    for row_number, line in enumerate(list_of_lines):
        # For each line, check if line contains any string from the list of strings
        for key in index_start:
            if key in line:
                # If any string is found in line, then append that line along with line number in list
                row_numbers.append(row_number)
                i += 1
        if i % 2 != 0 or i != 0:
            if index_end in line:
                row_numbers.append(row_number)
                if not is_all:
                    break
    # Return list of tuples containing matched string, line numbers and lines where string is found

    if is_all:
        row_numbers = [row_numbers[num] - 3 if num % 2 else row_numbers[num] + 1 for num in range(len(row_numbers))]
        row_numbers[-1] = row_numbers[-1] + 1
    elif is_gamma:
        row_numbers[0] = row_numbers[0] + 2
        row_numbers[-1] = row_numbers[-1] - 2
    else:
        row_numbers[0] = row_numbers[0] + 7
        row_numbers[-1] = row_numbers[-1] - 3
    return row_numbers


def extract_rows(file_name, keys_of_row):
    """
    依据行号，截取数据块
    本操作应放在抽取数据列之前
    :param file_name: 数据文件名
    :param keys_of_row: 切割数据块所需行关键字
    :return: DataFrame，数据行
    """

    with codecs.open(file_name, 'r', encoding='utf-8') as read_obj:
        lines = read_obj.readlines()

    # 依据文件名，获取数据块起止行号
    row_numbers = row_numbers_of_block(lines, keys_of_row)
    # 按行号读取数据块
    row_start = row_numbers[0]
    row_end = row_numbers[1]
    lines_data = lines[row_start:row_end + 1]

    df_data = pd.DataFrame([data.split() for data in lines_data])

    return df_data


def filter_data(df_data, nuc_names):
    """
    依据核素名称，筛选数据行
    :param df_data: DataFrame, 计算结果
    :param nuc_names: List, 关键核素，如果为none，则取不为0的全部核素
    :return: DataFrame，关键核素的计算结果
    """
    if (nuc_names is not None) and (len(nuc_names) > 1):
        df_is_in_nuc_names = df_data[1].isin(nuc_names)
        df_output = df_data.loc[df_is_in_nuc_names]
    else:
        # Drop rows with all zeros in data.

        # The two lines below, does the exact same thing here.
        # Drop rows with all zeros. But the latter one is way much faster.
        # df_filter = df_density.iloc[df_density.apply(np.sum, axis=1).to_numpy().nonzero()]
        # df_filter = df_density.iloc[df_density.any(axis=1).to_numpy().nonzero()]

        df_nuc = df_data.iloc[:, [0, 1]]
        df_density = df_data.iloc[:, [2, 3]]
        df_density = df_density.applymap(Decimal)

        # When only condition is provided,
        # the numpy.where() function is a shorthand for np.asarray(condition).nonzero().
        # Using nonzero directly should be preferred, as it behaves correctly for subclasses.
        # The rest of this documentation covers only the case where all three arguments are provided.
        # But to be clear, I leave the np.where() kind of function in this comment.
        # df_is_not_zero = np.where(df_density.any(axis=1))
        df_is_not_zero = df_density.any(axis=1).to_numpy().nonzero()

        df_output = pd.concat([df_nuc.iloc[df_is_not_zero], df_density.iloc[df_is_not_zero]], axis=1, copy=False)
        # df_output = df_data.loc[(df_density != Decimal(0)).any(axis=1), :]
    return df_output


def extract_columns(data_columns, step_numbers, is_all_step):
    """
    依据列号，抽取数据
    本操作应放在抽取数据行之后
    :param data_columns:数据行
    :param step_numbers:列号
    :param is_all_step: bool, 提取全部列，or 最后一列，默认取最后一列
    :return:DataFrame，数据
    """
    # 第0-1列为核素编号、名称
    # 第2列为初始核素信息
    # 最后一列为最终结果
    # 其他列为中间计算结果
    steps = [0, 1]

    # 是否输出全部燃耗步结果
    if is_all_step:
        # 第3 - 21列为全部燃耗步计算结果
        for i in range(len(data_columns.columns) - 4):
            steps.append(i + 3)
        # 更改列名
        # df_allstep.columns = list(np.arrange(1, 21))
    elif step_numbers:
        # 修改列号与输出文件的数据结构对应
        # 第0，1列为key，第2列为初始核素密度
        # 因此，step 1对应第3列
        for i in range(len(step_numbers)):
            # TODO fix this bug
            steps.append(step_numbers[i] + 2)
    # 最终结果列
    steps.append(-1)
    df_all_step = data_columns.iloc[:, steps]
    return df_all_step
    #
    # # 取核素列与最终结果列
    # df_nuc = data_columns.iloc[:, 0:2]
    # df_final = pd.DataFrame(data_columns.iloc[:, -1])
    # # 更改列名
    # # df_nuc.columns = ['NucId', 'NucName']
    # # df_final.columns = ['Final']
    #
    #
    # # 如果没有中间结果
    # if df_all_step is not None:
    #     # 拼接核素名与中间结果
    #     result = pd.concat([df_nuc, df_all_step], axis=1)
    # # 横向合并，即列拼接
    # result = pd.concat([result, df_final], axis=1)


def preprocess(file_path, keys_of_row, nuclide_list, keys_of_column, is_all_step, step_numbers):
    """
    数据预处理，读取指定数据写入csv
    :param file_path: 数据文件所在路径
    :param keys_of_row: 数据行起止关键字
    :param nuclide_list: 所需核素名称
    :param keys_of_column: 键所在列的列名，数据行的键值唯一，默认为id，name
    :param is_all_step: 是否读取全部中间结果数据列，默认只读取最终结果列
    :param step_numbers: 所属数据列号
    :return:
    """
    # 读取指定文件夹下的计算结果
    file_names = sorted(file_path.glob("*.out"))
    # 依次处理单个文件
    for file_name in file_names:
        # 提取数据行
        df_rows = extract_rows(file_name, keys_of_row)
        # 筛选关键核素所在行数据
        df_nuclide_list = filter_data(df_rows, nuclide_list)

        # 提取数据列
        df_columns = extract_columns(df_nuclide_list, step_numbers, is_all_step=is_all_step)
        # 修改列名
        # 读取列名
        columns = list(df_columns)
        # 最后一列为最终结果，列名为文件名
        column_name = file_name.stem.split('.')[0]
        columns[-1] = column_name + '_final'
        # 修改中间结果的列名
        if is_all_step:
            for i in range(len(columns) - 3):
                columns[i + 2] = column_name + '_step_' + str(i + 1)
        elif len(columns) > 3:
            for i in range(len(columns) - 3):
                columns[i + 2] = column_name + '_step_' + str(step_numbers[i])
        # 修改key列
        # 修改列名，默认第一列为核素id，第二列为核素名称
        columns[0] = keys_of_column['id']
        columns[1] = keys_of_column['name']

        df_columns.columns = columns
        # 保存到csv
        # df_columns.to_csv(file_name + '.csv', encoding='utf-8', index=False)
        df_columns.to_csv(f'{file_name}.csv', encoding='utf-8', index=False)


def merge_final_result(file_path, final_file_name='final'):
    """
    合并结果
    :param file_path: 中间结果csv所在路径
    :param final_file_name: 合并文件名
    :return: 无
    """
    # 读取指定文件夹下的计算结果
    file_names = sorted(file_path.glob("*.csv"))
    # 将第一个csv写入最终结果csv
    df_final = pd.read_csv(file_names[0])

    # 依次处理单个文件
    for i in range(1, len(file_names)):
        df_temp = pd.read_csv(file_names[i])
        # 合并
        df_final = pd.merge(df_final, df_temp)
    # 保存
    # df_final.to_csv(final_file_name + '.csv', encoding='utf-8', index=False)
    df_final.to_excel(final_file_name + '.xlsx', encoding='utf-8', index=False)


def process(file_path, physical_quantity, nuclide_list=None, step_numbers=None, is_all_step=False):
    """
    :param file_path:文件所在路径，相对py
    :param physical_quantity: 物理量类型
    :param nuclide_list:关键核素列表，None取全部核素
    :param step_numbers: 数据列索引
    :param is_all_step: 默认False，适用10step，True适用100step
    """

    # 核素ID和核素名对应的列名
    keys_of_column = configlib.Config.get_data_extraction_conf("keys_of_column")
    # 关键字，切分数据块
    keys_of_row = configlib.Config.get_data_extraction_conf("keys_of_rows").get(physical_quantity)
    # keys_of_split = ['NucID', 'Total', 'Energy']

    preprocess(file_path, keys_of_row, nuclide_list, keys_of_column, is_all_step=is_all_step, step_numbers=step_numbers)

    # 读取csv，合并结果
    merge_final_result(file_path)


def main():
    fission_light_nuclide_list = configlib.Config.get_nuclide_list("fission_light")
    test_file_path = configlib.Config.get_file_path("test_file_path")
    step_numbers = configlib.Config.get_data_extraction_conf("step_numbers")
    physical_quantity = "isotope"

    if step_numbers:
        is_all_step = True
    else:
        is_all_step = False

    process(file_path=test_file_path, nuclide_list=fission_light_nuclide_list,
            is_all_step=is_all_step, step_numbers=step_numbers, physical_quantity=physical_quantity)


if __name__ == '__main__':
    main()
