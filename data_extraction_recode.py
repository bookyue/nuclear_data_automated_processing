import pandas as pd

from utils.configlib import Config
from db.fetch_data import fetch_all_filenames, fetch_data_by_filename


def filter_data(dict_df_data, nuclide_list):
    """
    依据核素名称，筛选数据行
    :param dict_df_data: dictionary
    :param nuclide_list: list, 关键核素，如果为None，则取不为0的全部核素
    :return: DataFrame，关键核素的计算结果
    """
    if (nuclide_list is not None) and (len(nuclide_list) > 1):
        for key in dict_df_data:
            if key == 'gamma_spectra':
                continue
            is_in_nuclide_list = dict_df_data[key]['nuc_name'].isin(nuclide_list)
            dict_df_data[key]: pd.DataFrame = dict_df_data[key].loc[is_in_nuclide_list]
    else:
        # Drop rows with all zeros in data.
        for key in dict_df_data:
            """
            The two lines below, does the exact same thing here.
            Drop rows with all zeros. But the latter one is way much faster.
            df_filter = df_data.iloc[df_data.apply(np.sum, axis=1).to_numpy().nonzero()]
            df_filter = df_data.iloc[df_data.any(axis=1).to_numpy().nonzero()]

            When only condition is provided,
            the numpy.where() function is a shorthand for np.asarray(condition).nonzero().
            Using nonzero directly should be preferred, as it behaves correctly for subclasses.
            The rest of this documentation covers only the case where all three arguments are provided.
            But to be clear, I leave the np.where() kind of function in this comment.
            df_is_not_zero = np.where(df_density.any(axis=1))
            """

            df_is_not_zero = dict_df_data[key][['first_step', 'last_step']].any(axis=1).to_numpy().nonzero()
            dict_df_data[key] = dict_df_data[key].iloc[df_is_not_zero]
            # df_output = df_data.loc[(df_density != Decimal(0)).any(axis=1), :]

    return dict_df_data


# TODO wait for recoding
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


def process(physical_quantity_name, nuclide_list):
    filenames = fetch_all_filenames()
    for filename in filenames:
        # print(filename.name)
        dict_df_data = fetch_data_by_filename(filename, physical_quantity_name)
        df_nuclide_list = filter_data(dict_df_data, nuclide_list)
        # print(df_nuclide_list)


def main():
    fission_light_nuclide_list = Config.get_nuclide_list("fission_light")

    step_numbers = Config.get_data_extraction_conf("step_numbers")
    physical_quantity_name = "all"
    is_all_step = False

    process(physical_quantity_name=physical_quantity_name, nuclide_list=fission_light_nuclide_list)


if __name__ == '__main__':
    main()
