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
        if not file_path.exists():
            config_file_path = Path('..')
            file_path = Path.joinpath(config_file_path, 'config.toml')
        conf = toml.load(file_path)
    except FileNotFoundError:
        print("The config file doesn't exist!")

    @classmethod
    def get_conf(cls, properties):
        return cls.conf.get(properties)

    @classmethod
    def get_data_extraction_conf(cls, properties):
        return cls.conf.get("data_extraction").get(properties)

    @classmethod
    def get_file_path(cls, properties):
        return Path(cls.conf.get("file_path").get(properties))

    @classmethod
    def get_database_config(cls):
        return cls.conf.get("database")

    @classmethod
    def get_nuclide_list(cls, nuclide_name):
        return cls.conf.get("nuclide_list").get(nuclide_name, None)

    @classmethod
    def get_decay_nuclide_list(cls):
        return cls.conf.get("nuclide_list").get("decay")

    @classmethod
    def get_fission_light_nuclide_list(cls):
        return cls.conf.get("nuclide_list").get("fission_light")

    @classmethod
    def get_short_lives_nuclide_list(cls):
        return cls.conf.get("nuclide_list").get("short_lives")

