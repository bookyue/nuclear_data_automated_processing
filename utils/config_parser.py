import os

import toml


def read_config(config_file_path):
    try:
        config = toml.load(f'{config_file_path}/config.toml')
        print(config)
        return config
    except FileNotFoundError:
        print("The config file doesn't exist!")
