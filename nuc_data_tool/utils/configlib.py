from pathlib import Path

import toml


class Config:
    """
    配置文件类
    """

    def __init__(self, path):
        try:
            config_file_path = Path(path)
            self.conf = toml.load(config_file_path)
        except FileNotFoundError:
            print("The config file doesn't exist!")

    def get_conf(self, properties):
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
        return self.conf.get(properties)

    def get_data_extraction_conf(self, properties):
        """
        获取data_extraction下的配置

        Parameters
        ----------
        properties : str
            字段

        Returns
        -------
        dict
        """
        return self.conf.get("data_extraction").get(properties)

    def get_file_path(self, properties):
        """
        获取文件路径

        Parameters
        ----------
        properties : str
            字段

        Returns
        -------
        Path
        """
        return Path(self.conf.get("file_path").get(properties))

    def get_anomaly_detection_config(self, properties):
        """
        获取异常检测的配置
        Returns
        -------

        """
        if isinstance(self.conf.get("anomaly_detection").get(properties), str):
            return Path(self.conf.get("anomaly_detection").get(properties))
        else:
            return self.conf.get("anomaly_detection").get(properties)

    def get_database_config(self):
        """
        获取database下的配置

        Returns
        -------
        dict
        """
        return self.conf.get("database")

    def get_nuclide_list(self, nuclide_name):
        """
        输入核素列表名，获取对应的核素列表
        Parameters
        ----------
        nuclide_name : str
            核素列表名
        Returns
        -------
        list[str]
        """
        return self.conf.get("nuclide_list").get(nuclide_name, None)


CONFIG_FILE_PATH = Path.cwd().joinpath('config.toml')
if not CONFIG_FILE_PATH.exists():
    CONFIG_FILE_PATH = Path(__file__).parent.parent.joinpath('config.toml')

config = Config(CONFIG_FILE_PATH)
