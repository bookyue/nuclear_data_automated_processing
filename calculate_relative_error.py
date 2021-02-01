from pathlib import Path

from utils import config_parser
from utils.worksheet import get_column_index


def main():
    config_file_path = Path('.')
    config: dict = config_parser.read_config(config_file_path)

    file_path = Path(config.get("file").get("file_path"))
    filenames = file_path.glob('*.xlsx')
    for filename in filenames:
        # print(filename)
        column_index = get_column_index(filename)
        print(column_index)


if __name__ == '__main__':
    main()
