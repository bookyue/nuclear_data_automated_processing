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
              help='物理量，默认为全部物理量')
@click.option('--initiation', '-init',
              'initiation',
              is_flag=True,
              default=False,
              help='初始化数据库')
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


@cli.command()
@click.option('--file', '-f',
              'filenames',
              default=['all'],
              multiple=True,
              help='文件名(没有后缀) 例如：001.xml.out -> 001，默认为所有文件')
@click.option('--result_path', '-p'
              'result_path',
              default=config.get_file_path('result_file_path'),
              type=click.Path(exists=True),
              help='输出文件路径，默认读取配置文件中的路径')
@click.option('--physical_quantities', '-pq',
              'physical_quantities',
              default=all_physical_quantity_list,
              type=click.Choice(all_physical_quantity_list,
                                case_sensitive=False),
              multiple=True,
              help='物理量，默认为全部物理量')
@click.option('--nuclide', '-n',
              'nuclide_list',
              type=click.Choice(config.get_conf('nuclide_list').keys(),
                                case_sensitive=False),
              help='核素列表，从配置文件 nuclide_list 项下读取')
@click.option('--all_step', '-all',
              'is_all_step',
              is_flag=True,
              default=False,
              help='提取中间步骤')
def extract(filenames,
            result_path,
            physical_quantities,
            nuclide_list,
            is_all_step):
    """
    从数据库导出选中的文件的数据到工作簿(xlsx文件)
    """
    if len(filenames) == 1:
        (filenames,) = filenames

    process(filenames=filenames,
            result_path=result_path,
            physical_quantities=physical_quantities,
            nuclide_list=config.get_nuclide_list(nuclide_list),
            is_all_step=is_all_step)


def main():
    cli()


main()
