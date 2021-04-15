from pathlib import Path

import click
from click import UsageError

from nuc_data_tool import __version__
from nuc_data_tool.db.db_utils import init_db
from nuc_data_tool.db.fetch_data import (fetch_extracted_data_id,
                                         fetch_physical_quantities_by_name,
                                         fetch_files_by_name)
from nuc_data_tool.utils.configlib import config
from nuc_data_tool.utils.data_extraction import save_extracted_data_to_exel
from nuc_data_tool.utils.fill_db import populate_database
from nuc_data_tool.utils.formatter import (all_physical_quantity_list,
                                           physical_quantity_list_generator)
from nuc_data_tool.utils.input_xml_file import InputXmlFileReader
from nuc_data_tool.utils.relative_error_calculation import save_comparative_result_to_excel


class PythonLiteralOption(click.Option):

    def type_cast_value(self, ctx, value):
        return list(map(str.strip, value.strip('][').replace('"', '').split(',')))


class MutuallyExclusiveOption(click.Option):
    def __init__(self, *args, **kwargs):
        self.mutually_exclusive = set(kwargs.pop('mutually_exclusive', []))
        help_args = kwargs.get('help', '')
        if self.mutually_exclusive:
            ex_str = ', '.join(self.mutually_exclusive)
            kwargs['help'] = help_args + (
                    ' NOTE: This argument is mutually exclusive with '
                    ' arguments: [' + ex_str + '].'
            )
        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        if self.mutually_exclusive.intersection(opts) and self.name in opts:
            raise UsageError(
                "Illegal usage: `{}` is mutually exclusive with "
                "arguments `{}`.".format(
                    self.name,
                    ', '.join(self.mutually_exclusive)
                )
            )

        return super(MutuallyExclusiveOption, self).handle_parse_result(
            ctx,
            opts,
            args
        )


@click.group()
@click.version_option(__version__)
def main_cli():
    """
    app 命令行
    """
    pass


@main_cli.command()
@click.option('--path', '-p',
              'path',
              default=config.get_file_path('test_file_path'),
              type=click.Path(),
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

    file_names = sorted(Path(path).glob('*.out'))
    for file_name in file_names:
        with InputXmlFileReader(file_name, physical_quantities) as xml_file:
            print(f'{xml_file.name}:')
            print(f'found:     {xml_file.fetched_physical_quantity}')
            print(f'not found: {xml_file.unfetched_physical_quantity}')
            print()
            populate_database(xml_file)


@main_cli.command()
@click.option('--files', '-f',
              'filenames',
              default='all',
              cls=PythonLiteralOption,
              help="""文件名(列表)(没有后缀) 例如：001.xml.out -> 001，默认为所有文件
                   例子： 003  001,002  '[001,003]'""")
@click.option('--result_path', '-p',
              'result_path',
              default=config.get_file_path('result_file_path'),
              type=click.Path(),
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


@main_cli.command()
@click.option('--files', '-f',
              'filenames',
              default='all',
              cls=PythonLiteralOption,
              help="""文件名(列表)(没有后缀) 例如：001.xml.out -> 001，默认为所有文件
                   例子： 003  001,002,003  '[001,004,003]'""")
@click.option('--result_path', '-p',
              'result_path',
              default=config.get_file_path('result_file_path'),
              type=click.Path(),
              help='输出文件路径，默认读取配置文件中的路径')
@click.option('--physical_quantities', '-pq',
              'physical_quantities',
              default=['isotope'],
              type=click.Choice(all_physical_quantity_list,
                                case_sensitive=False),
              multiple=True,
              help='物理量，默认为 isotope')
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


@main_cli.command()
@click.option('--file', '-f',
              'files',
              cls=MutuallyExclusiveOption,
              is_flag=True,
              default=False,
              mutually_exclusive=['physical_quantity'],
              help='显示文件信息')
@click.option('--physical_quantity', '-p',
              'physical_quantities',
              cls=MutuallyExclusiveOption,
              is_flag=True,
              default=False,
              mutually_exclusive=['files'],
              help='显示物理量信息')
@click.option('--list', '-l',
              'is_list',
              is_flag=True,
              default=False,
              help='以数组形式输出')
def fetch(files,
          physical_quantities,
          is_list):
    """
    获取 文件、物理量信息
    """

    if files is True:
        file_list = fetch_files_by_name('all')
        if is_list is False:
            for file in file_list:
                print(f'Name: {file.name}')
        else:
            print([file.name for file in file_list])

    if physical_quantities is True:
        physical_quantity_list = fetch_physical_quantities_by_name('all')
        if is_list is False:
            for physical_quantity in physical_quantity_list:
                print(f'Name: {physical_quantity.name}')
        else:
            print([physical_quantity.name for physical_quantity in physical_quantity_list])


def main():
    main_cli(prog_name='python -m nuc_data_tool')


main()
