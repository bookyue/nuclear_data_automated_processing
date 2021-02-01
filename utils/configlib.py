from pathlib import Path

import toml


class Config:
    # def __init__(self, config_file_path='.'):
    #     try:
    #         self.file_path = Path(f'{config_file_path}/config.toml')
    #         self.conf = toml.load(self.file_path)
    #     except FileNotFoundError:
    #         print("The config file doesn't exist!")
    try:
        config_file_path = Path('.')
        file_path = Path.joinpath(config_file_path, 'config.toml')
        conf = toml.load(file_path)
    except FileNotFoundError:
        print("The config file doesn't exist!")

    @classmethod
    def get_file_path(cls):
        return Path(cls.conf.get("file").get("file_path"))

    @classmethod
    def get_conf(cls, properties):
        return cls.conf.get(properties)
