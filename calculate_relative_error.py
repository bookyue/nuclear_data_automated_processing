from pathlib import Path

from utils import configlib
from utils.worksheet import get_column_index


def main():

    file_path = configlib.Config.get_file_path()
    file_path = Path(configlib.Config.get_conf("file").get("file_path"))

    filenames = file_path.glob('*.xlsx')
    for filename in filenames:
        # print(filename)
        column_index = get_column_index(filename)
        print(column_index)


if __name__ == '__main__':
    main()
