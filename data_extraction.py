from db.db_model import ExtractedData
from db.db_utils import delete_all_from_table
from db.fetch_data import fetch_data_by_filename_and_nuclide_list, fetch_files_by_name, \
    fetch_physical_quantities_by_name
from db.save_data import save_extracted_data_to_db, save_extracted_data_to_exel
from utils.configlib import Config


def filter_data(filename, physical_quantity_name, nuclide_list, is_all_step):
    """
    依据核素名称，筛选数据行
    :param is_all_step:
    :param filename: File object
    :param physical_quantity_name: str or list or PhysicalQuantity object
    :param nuclide_list: list, 关键核素，如果为None，则取不为0的全部核素
    :return: DataFrame，关键核素的计算结果
    """
    # if (nuclide_list is not None) and (len(nuclide_list) > 1):
    #     for key in dict_df_data:
    #         if key == 'gamma_spectra':
    #             continue
    #         is_in_nuclide_list = dict_df_data[key]['nuc_name'].isin(nuclide_list)
    #         dict_df_data[key]: pd.DataFrame = dict_df_data[key].loc[is_in_nuclide_list]
    # else:
    #     # Drop rows with all zeros in data.
    #     for key in dict_df_data:
    #         """
    #         The two lines below, does the exact same thing here.
    #         Drop rows with all zeros. But the latter one is way much faster.
    #         df_filter = df_data.iloc[df_data.apply(np.sum, axis=1).to_numpy().nonzero()]
    #         df_filter = df_data.iloc[df_data.any(axis=1).to_numpy().nonzero()]
    #
    #         When only condition is provided,
    #         the numpy.where() function is a shorthand for np.asarray(condition).nonzero().
    #         Using nonzero directly should be preferred, as it behaves correctly for subclasses.
    #         The rest of this documentation covers only the case where all three arguments are provided.
    #         But to be clear, I leave the np.where() kind of function in this comment.
    #         df_is_not_zero = np.where(df_density.any(axis=1))
    #         """
    #
    #         df_is_not_zero = dict_df_data[key][['first_step', 'last_step']].any(axis=1).to_numpy().nonzero()
    #         dict_df_data[key] = dict_df_data[key].iloc[df_is_not_zero]
    #         # df_output = df_data.loc[(df_density != Decimal(0)).any(axis=1), :]
    dict_df_data = fetch_data_by_filename_and_nuclide_list(filename, physical_quantity_name, nuclide_list, is_all_step)

    return dict_df_data


def process(physical_quantity_name, nuclide_list, is_all_step):
    filenames = fetch_files_by_name()
    physical_quantities = fetch_physical_quantities_by_name(physical_quantity_name)
    file_path = Config.get_file_path('result_file_path')

    delete_all_from_table(ExtractedData)

    save_extracted_data_to_db(filenames, physical_quantities, nuclide_list)

    save_extracted_data_to_exel(filenames, is_all_step, file_path)


def main():
    fission_light_nuclide_list = Config.get_nuclide_list('fission_light')
    is_all_step = Config.get_data_extraction_conf('is_all_step')
    physical_quantity_name = 'all'

    process(physical_quantity_name=physical_quantity_name,
            nuclide_list=fission_light_nuclide_list,
            is_all_step=is_all_step)


if __name__ == '__main__':
    main()
