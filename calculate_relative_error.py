from utils import configlib


def main():
    file_path = configlib.Config.get_file_path("burnup_time_file_path")
    # file_path = configlib.Config.get_file_path("test_file_path")

    #
    # filenames = file_path.glob('*.xlsx')
    # for file_name in filenames:
    #     # print(file_name)
    #     column_index = get_column_index(file_name)
    #     print(column_index)

    # decay_nuclide_list = configlib.Config.get_decay_nuclide_list()
    # short_lives_nuclide_list = configlib.Config.get_nuclide_list("short_lives")
    # print(short_lives_nuclide_list)


if __name__ == '__main__':
    main()
