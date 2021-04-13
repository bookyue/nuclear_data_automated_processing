import click

from utils.data_extraction import save_extracted_data_to_exel
from db.db_utils import init_db
from db.fetch_data import fetch_extracted_data_id, fetch_physical_quantities_by_name, fetch_files_by_name
from utils.fill_db import populate_database
from utils.relative_error_calculation import save_comparative_result_to_excel
from utils.configlib import config
from utils.formatter import all_physical_quantity_list, physical_quantity_list_generator
from utils.input_xml_file import InputXmlFileReader


class PythonLiteralOption(click.Option):

    def type_cast_value(self, ctx, value):
        return list(map(str.strip, value.strip('][').replace('"', '').split(',')))


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
              default='all',
              cls=PythonLiteralOption,
              help="""文件名(列表)(没有后缀) 例如：001.xml.out -> 001，默认为所有文件
                   例子： 003  001,002  '[001,003]'""")
@click.option('--result_path', '-p',
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
              default='fission_light',
              type=click.Choice(config.get_conf('nuclide_list').keys(),
                                case_sensitive=False),
              help='核素列表，从配置文件 nuclide_list 项下读取，默认 fission_light')
@click.option('--all_step', '-all',
              'is_all_step',
              is_flag=True,
              default=False,
              help='提取中间步骤')
@click.option('--merge', '-m',
              'merge',
              is_flag=True,
              default=False,
              help='将结果合并输出至一个文件')
def extract(filenames,
            result_path,
            physical_quantities,
            nuclide_list,
            is_all_step,
            merge):
    """
    从数据库导出选中的文件的数据到工作簿(xlsx文件)
    """

    filenames = fetch_files_by_name(filenames)
    physical_quantities = fetch_physical_quantities_by_name(physical_quantities)
    nuc_data_id = fetch_extracted_data_id(filenames,
                                          physical_quantities,
                                          config.get_nuclide_list(nuclide_list))

    save_extracted_data_to_exel(nuc_data_id=nuc_data_id,
                                filenames=filenames,
                                physical_quantities=physical_quantities,
                                is_all_step=is_all_step,
                                result_path=result_path,
                                merge=merge)


@cli.command()
@click.option('--file', '-f',
              'filenames',
              default='all',
              cls=PythonLiteralOption,
              help="""文件名(列表)(没有后缀) 例如：001.xml.out -> 001，默认为所有文件
                   例子： 003  001,002,003  '[001,004,003]'""")
@click.option('--result_path', '-p',
              'result_path',
              default=config.get_file_path('result_file_path'),
              type=click.Path(exists=True),
              help='输出文件路径，默认读取配置文件中的路径')
@click.option('--physical_quantities', '-pq',
              'physical_quantities',
              default=['isotope'],
              type=click.Choice(all_physical_quantity_list,
                                case_sensitive=False),
              multiple=True,
              help='物理量，默认为全部物理量')
@click.option('--nuclide', '-n',
              'nuclide_list',
              default='fission_light',
              type=click.Choice(config.get_conf('nuclide_list').keys(),
                                case_sensitive=False),
              help='核素列表，从配置文件 nuclide_list 项下读取')
@click.option('--deviation_mode',
              '-dm',
              'deviation_mode',
              default='relative',
              type=click.Choice(['relative', 'absolute']),
              help='偏差模式，分为绝对和相对，默认为相对')
@click.option('--threshold',
              '-t',
              'threshold',
              default='1.0E-12',
              help='偏差阈值，默认1.0E-12')
@click.option('--all_step', '-all',
              'is_all_step',
              is_flag=True,
              default=False,
              help='提取中间步骤')
def compare(filenames,
            result_path,
            physical_quantities,
            nuclide_list,
            deviation_mode,
            threshold,
            is_all_step):
    """
    对文件列表进行两两组合，进行对比，计算并输出对比结果至工作簿(xlsx文件)
    """

    filenames = fetch_files_by_name(filenames)
    physical_quantities = fetch_physical_quantities_by_name(physical_quantities)
    nuc_data_id = fetch_extracted_data_id(filenames,
                                          physical_quantities,
                                          config.get_nuclide_list(nuclide_list))

    save_comparative_result_to_excel(nuc_data_id=nuc_data_id,
                                     files=filenames,
                                     result_path=result_path,
                                     physical_quantities=physical_quantities,
                                     deviation_mode=deviation_mode,
                                     threshold=threshold,
                                     is_all_step=is_all_step)


def main():
    cli()


main()
