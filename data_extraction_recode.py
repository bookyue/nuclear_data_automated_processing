import codecs
from decimal import Decimal

import pandas as pd
# from cyberbrain import trace

from utils import configlib


def row_numbers_of_physical_quantity(file_name, physical_quantity_name):
    list_of_strings_to_search = configlib.Config.get_data_extraction_conf("keys_of_rows").get(physical_quantity_name)
    physical_quantity_list = configlib.physical_quantity_list_generator(physical_quantity_name)

    index_start = list_of_strings_to_search[:-1]
    index_end = list_of_strings_to_search[-1]

    length_of_physical_quantity = {'isotope': [],
                                   'radioactivity': [],
                                   'absorption': [],
                                   'fission': [],
                                   'decay_heat': [],
                                   'gamma_spectra': []
                                   }

    i = -1
    is_find_start_title = False
    with codecs.open(file_name, 'r', encoding='utf-8') as file:
        for row_number, line in enumerate(file):
            if not is_find_start_title:
                for string_to_search in index_start:
                    if string_to_search in line:
                        i += 1
                        is_find_start_title = True
                        length_of_physical_quantity[physical_quantity_list[i]].append(
                            row_number + 7 if physical_quantity_list[i] != 'gamma_spectra' else row_number + 2)

            if is_find_start_title:
                if index_end in line:
                    is_find_start_title = False
                    length_of_physical_quantity[physical_quantity_list[i]].append(
                        row_number - 3 if physical_quantity_list[i] != 'gamma_spectra' else row_number - 2)
                    if physical_quantity_name != 'all':
                        break
    print(file_name)
    print(length_of_physical_quantity)


def process(file_path, physical_quantity_all):
    # 核素ID和核素名对应的列名
    keys_of_column = configlib.Config.get_data_extraction_conf("keys_of_column")

    file_names = file_path.glob("*.out")
    for file_name in file_names:
        row_numbers_of_physical_quantity(file_name, physical_quantity_all)


def main():
    fission_light_nuclide_list = configlib.Config.get_nuclide_list("fission_light")
    test_file_path = configlib.Config.get_file_path("test_file_path")
    step_numbers = configlib.Config.get_data_extraction_conf("step_numbers")
    physical_quantity_name = "all"
    is_all_step = False

    process(file_path=test_file_path, physical_quantity_all=physical_quantity_name)


if __name__ == '__main__':
    main()
