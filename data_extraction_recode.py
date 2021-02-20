import pandas as pd

from utils import configlib
from utils.input_xml_file import InputXmlFileReader


def extract_rows(xml_file):
    df_data = []
    file_lines = xml_file.file_object.readlines()
    for key in xml_file.chosen_physical_quantity:
        row_start = xml_file.length_of_physical_quantity[key][0]
        row_end = xml_file.length_of_physical_quantity[key][1]
        text = file_lines[row_start:row_end + 1]
        df_data.append(pd.DataFrame([data.split() for data in text]))
    return df_data


def process(file_path, physical_quantity_name):
    # 核素ID和核素名对应的列名
    keys_of_column = configlib.Config.get_data_extraction_conf("keys_of_column")

    file_names = file_path.glob("*.out")
    for file_name in file_names:
        with InputXmlFileReader(file_name, physical_quantity_name) as xml_file:
            print(xml_file.name)
            print(xml_file.chosen_physical_quantity)
            print(xml_file.unfetched_physical_quantity)
            print(xml_file.length_of_physical_quantity)
            df_data = extract_rows(xml_file)


def main():
    fission_light_nuclide_list = configlib.Config.get_nuclide_list("fission_light")
    test_file_path = configlib.Config.get_file_path("test_file_path")
    step_numbers = configlib.Config.get_data_extraction_conf("step_numbers")
    physical_quantity_name = "all"
    is_all_step = False

    process(file_path=test_file_path, physical_quantity_name=physical_quantity_name)


if __name__ == '__main__':
    main()
