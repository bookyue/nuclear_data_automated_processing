from decimal import Decimal

import pandas as pd

from utils import configlib
from utils.input_xml_file import InputXmlFileReader


def filter_data(dict_text, nuclide_list):
    """
    依据核素名称，筛选数据行
    :param dict_text: dictionary
    :param nuclide_list: list, 关键核素，如果为None，则取不为0的全部核素
    :return: DataFrame，关键核素的计算结果
    """
    dict_df_data = {key: pd.DataFrame([data.split() for data in dict_text[key]])
                    for key in dict_text}
    if (nuclide_list is not None) and (len(nuclide_list) > 1):
        for key in dict_df_data:
            # dict_df_data[key] = dict_df_data[key]
            is_in_nuclide_list = dict_df_data[key].iloc[:, 1].isin(nuclide_list)
            if key == 'gamma_spectra':
                continue
            dict_df_data[key]: pd.DataFrame = dict_df_data[key].loc[is_in_nuclide_list]
    else:
        # Drop rows with all zeros in data.

        for key in dict_df_data:
            if key == 'gamma_spectra':
                continue
            df_nuc: pd.DataFrame = dict_df_data[key].iloc[:, [0, 1]]
            df_data: pd.DataFrame = dict_df_data[key].iloc[:, [2, 3]]
            df_data = df_data.applymap(Decimal)

            # The two lines below, does the exact same thing here.
            # Drop rows with all zeros. But the latter one is way much faster.
            # df_filter = df_data.iloc[df_data.apply(np.sum, axis=1).to_numpy().nonzero()]
            # df_filter = df_data.iloc[df_data.any(axis=1).to_numpy().nonzero()]

            # When only condition is provided,
            # the numpy.where() function is a shorthand for np.asarray(condition).nonzero().
            # Using nonzero directly should be preferred, as it behaves correctly for subclasses.
            # The rest of this documentation covers only the case where all three arguments are provided.
            # But to be clear, I leave the np.where() kind of function in this comment.
            # df_is_not_zero = np.where(df_density.any(axis=1))

            df_is_not_zero = df_data.any(axis=1).to_numpy().nonzero()
            dict_df_data[key] = pd.concat([df_nuc.iloc[df_is_not_zero], df_data.iloc[df_is_not_zero]],
                                          axis=1, copy=False)
            # df_output = df_data.loc[(df_density != Decimal(0)).any(axis=1), :]

    return dict_df_data


def process(file_path, physical_quantity_name, nuclide_list):
    # 核素ID和核素名对应的列名
    keys_of_column = configlib.Config.get_data_extraction_conf('keys_of_column')

    file_names = file_path.glob('*.out')
    for file_name in file_names:
        with InputXmlFileReader(file_name, physical_quantity_name) as xml_file:
            dict_text = xml_file['all']
            df_nuclide_list = filter_data(dict_text, nuclide_list)

            # print(xml_file.name)
            # print(xml_file.chosen_physical_quantity)
            # print(xml_file.unfetched_physical_quantity)
            # print(xml_file.length_of_physical_quantity)


def main():
    fission_light_nuclide_list = configlib.Config.get_nuclide_list("fission_light")
    test_file_path = configlib.Config.get_file_path("test_file_path")
    step_numbers = configlib.Config.get_data_extraction_conf("step_numbers")
    physical_quantity_name = "all"
    is_all_step = False

    process(file_path=test_file_path, physical_quantity_name=physical_quantity_name,
            nuclide_list=fission_light_nuclide_list)


if __name__ == '__main__':
    main()
