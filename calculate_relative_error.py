from utils import configlib
from utils.worksheet import get_column_index


def main():
    file_path = configlib.Config.get_file_path("burnup_time_file_pth")
    # file_path = Path(configlib.Config.get_conf("file").get("file_path"))
    #
    # filenames = file_path.glob('*.xlsx')
    # for filename in filenames:
    #     # print(filename)
    #     column_index = get_column_index(filename)
    #     print(column_index)

    # decay_nuclide_list = configlib.Config.get_decay_nuclide_list()
    # short_lives_nuclide_list = configlib.Config.get_nuclide_list("short_lives")
    # print(short_lives_nuclide_list)


if __name__ == '__main__':
    main()
