from utils import configlib
from utils.input_xml_file import InputXmlFile


def extract_rows(xml_file):
    # text = []
    # for key in physical_quantity_list:
    #     row_start = row_numbers[key][0]
    #     row_end = row_numbers[key][1]
    #     text.append(file_lines[row_start:row_end + 1])
    # test = {key: value for key, value in zip(physical_quantity_list, text)}
    # print(test)
    # print(file_name)
    text = []
    for key in xml_file.chosen_physical_quantity:
        pass


def process(file_path, physical_quantity_name):
    # 核素ID和核素名对应的列名
    keys_of_column = configlib.Config.get_data_extraction_conf("keys_of_column")

    file_names = file_path.glob("*.out")
    for file_name in file_names:
        xml_file = InputXmlFile(file_name, physical_quantity_name)
        print(xml_file.name)
        print(xml_file.chosen_physical_quantity)
        print(xml_file.unfetched_physical_quantity)
        print(xml_file.length_of_physical_quantity)
        extract_rows(xml_file)


def main():
    fission_light_nuclide_list = configlib.Config.get_nuclide_list("fission_light")
    test_file_path = configlib.Config.get_file_path("test_file_path")
    step_numbers = configlib.Config.get_data_extraction_conf("step_numbers")
    physical_quantity_name = "gamma_spectra"
    is_all_step = False

    process(file_path=test_file_path, physical_quantity_name=physical_quantity_name)


if __name__ == '__main__':
    main()
