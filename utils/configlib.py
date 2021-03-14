from pathlib import Path

import toml


class Config:
    """
    配置文件类
    """
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
        """
        获取整个配置文件

        Parameters
        ----------
        properties : str
            字段

        Returns
        -------
        dict
        """
        return cls.conf.get(properties)

    @classmethod
    def get_data_extraction_conf(cls, properties):
        """
        获取data_extraction下的配置

        Parameters
        ----------
        properties : str
            字段

        Returns
        -------
        """
        return cls.conf.get("data_extraction").get(properties)

    @classmethod
    def get_file_path(cls, properties):
        """
        获取data_extraction下的配置

        Parameters
        ----------
        properties : str
            字段

        Returns
        -------

        """
        return Path(cls.conf.get("file_path").get(properties))

    @classmethod
    def get_database_config(cls):
        """
        获取database下的配置

        Returns
        -------

        """
        return cls.conf.get("database")

    @classmethod
    def get_nuclide_list(cls, nuclide_name):
        """
        获取nuclide_list下的配置
        Parameters
        ----------
        nuclide_name : 核素名

        Returns
        -------

        """
        return cls.conf.get("nuclide_list").get(nuclide_name, None)

    @classmethod
    def get_decay_nuclide_list(cls):
        """
        获取decay核素列表
        Returns
        -------

        """
        return cls.conf.get("nuclide_list").get("decay")

    @classmethod
    def get_fission_light_nuclide_list(cls):
        """
        获取fission_light核素列表
        Returns
        -------

        """
        return cls.conf.get("nuclide_list").get("fission_light")

    @classmethod
    def get_short_lives_nuclide_list(cls):
        """
        获取short_lives核素列表
        Returns
        -------

        """
        return cls.conf.get("nuclide_list").get("short_lives")
