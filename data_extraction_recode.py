import codecs
from decimal import Decimal

import pandas as pd
# from cyberbrain import trace

from utils import configlib


# def get_physical_quantity_key_from_value(value_to_search):
#     physical_quantity_dict = configlib.Config.get_data_extraction_conf("keys_of_rows")
#     return next((key for key, value in physical_quantity_dict.items() if value == value_to_search), None)


def row_numbers_of_physical_quantity(file_name, physical_quantity_list):
    list_of_strings_to_search = configlib.Config.get_data_extraction_conf("keys_of_rows").get(physical_quantity_list)
    is_all = False
    if len(list_of_strings_to_search) > 2:
        is_all = True

    index_start = list_of_strings_to_search[:-1]
    index_end = list_of_strings_to_search[-1]

    is_gamma = False
    if physical_quantity_list == 'gamma_spectra':
        is_gamma = True

    # is_physical_quantity = {'isotope': False,
    #                         'radioactivity': False,
    #                         'absorption': False,
    #                         'fission': False,
    #                         'decay_heat': False,
    #                         'gamma_spectra': False
    #                         }

    length_of_physical_quantity = {'isotope': [],
                                   'radioactivity': [],
                                   'absorption': [],
                                   'fission': [],
                                   'decay_heat': [],
                                   'gamma_spectra': []
                                   }

    i = 0
    with codecs.open(file_name, 'r', encoding='utf-8') as file:
        for row_number, line in enumerate(file):
            for string_to_search in index_start:
                if string_to_search in line:
                    # key = get_physical_quantity_key_from_value([string_to_search, index_end])
                    # is_physical_quantity[key] = True
                    length_of_physical_quantity[physical_quantity_list].append(
                        row_number + 7 if physical_quantity_list != 'gamma_spectra ' else row_number + 2)
                    i += 1
            if i % 2 != 0 or i != 0:
                if index_end in line:
                    length_of_physical_quantity[physical_quantity_list].append(
                        row_number - 3 if physical_quantity_list != 'gamma_spectra' else row_number - 2)
                    if not is_all:
                        break
    print(file_name)
    # print(is_physical_quantity)
    print(length_of_physical_quantity)


def process(file_path, physical_quantity_list):
    # 核素ID和核素名对应的列名
    keys_of_column = configlib.Config.get_data_extraction_conf("keys_of_column")

    file_names = file_path.glob("*.out")
    for file_name in file_names:
        row_numbers_of_physical_quantity(file_name, physical_quantity_list)


def main():
    fission_light_nuclide_list = configlib.Config.get_nuclide_list("fission_light")
    test_file_path = configlib.Config.get_file_path("test_file_path")
    step_numbers = configlib.Config.get_data_extraction_conf("step_numbers")
    physical_quantity_list = "isotope"
    is_all_step = False

    process(file_path=test_file_path, physical_quantity_list=physical_quantity_list)


if __name__ == '__main__':
    main()
