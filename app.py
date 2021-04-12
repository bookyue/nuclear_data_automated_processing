import click

from data_extraction import process
from db.db_utils import init_db
from fill_db import populate_database
from utils.configlib import config
from utils.formatter import all_physical_quantity_list, physical_quantity_list_generator
from utils.input_xml_file import InputXmlFileReader


@click.group()
def cli():
    """
    app 命令行
    """
    pass


@cli.command()
@click.option('--path', '-p',
              'path',
              default=config.get_file_path('test_file_path'),
              type=click.Path(exists=True),
              help='输入文件路径，默认读取配置文件中的路径')
@click.option('--physical_quantities', '-pq',
              'physical_quantities',
              default=all_physical_quantity_list,
              type=click.Choice(all_physical_quantity_list,
                                case_sensitive=False),
              multiple=True,
              help='物理量')
@click.option('--initiation', '-init',
              'initiation',
              is_flag=True,
              default=False,
              help='是否初始化数据库，默认否')
def pop(path,
        physical_quantities,
        initiation):
    """
    将输入文件(*.xml.out) 的内容填充进数据库
    """

    if initiation is True:
        init_db()

    physical_quantities = physical_quantity_list_generator(physical_quantities)
    file_names = sorted(path.glob('*.out'))
    for file_name in file_names:
        with InputXmlFileReader(file_name, physical_quantities) as xml_file:
            print(f'{xml_file.name}:')
            print(f'found:     {xml_file.chosen_physical_quantity}')
            print(f'not found: {xml_file.unfetched_physical_quantity}')
            print()
            populate_database(xml_file)


def main():
    cli()


main()
