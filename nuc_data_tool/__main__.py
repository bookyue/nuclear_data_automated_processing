from pathlib import Path

import click

from nuc_data_tool import __version__
from nuc_data_tool.anomaly_detection.iforest import save_prediction_to_exel
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
from nuc_data_tool.utils.relative_error_calculation import save_comparison_result_to_excel


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
            raise click.UsageError(
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
              help='输出文件路径，默认读取配置文件中的路径')
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
    将输出文件(*.xml.out) 的内容填充进数据库
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
@click.argument('filenames',
                nargs=-1)
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

    参数为文件列表(默认为所有文件)

    \b
    nuc_data_tool extract 'homo-case001-006' 'homo-case007-012' 'homo-case013-018'
    nuc_data_tool extract 'homo-case001-006'
    \b
    文件名(没有后缀) 例如：001.xml.out -> 001
    文件名列表 例如： 001 002 003
    """

    if filenames:
        filenames = fetch_files_by_name(filenames)
    else:
        filenames = fetch_files_by_name('all')

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
@click.argument('reference_file',
                nargs=1)
@click.argument('comparison_files',
                nargs=-1)
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
def compare(reference_file,
            comparison_files,
            result_path,
            physical_quantities,
            nuclide_list,
            deviation_mode,
            threshold,
            is_all_step):
    """
    \b
    对文件列表进行两两组合，进行对比，计算并输出对比结果至工作簿(xlsx文件)
    第一个参数为基准文件，第二个参数为文件列表(默认为除基准文件以外的所有文件)
    \b
    nuc_data_tool compare 'homo-case001-006' 'homo-case007-012' 'homo-case013-018'
    nuc_data_tool compare 'homo-case001-006'
    \b
    文件名(没有后缀) 例如：001.xml.out -> 001
    文件名列表 例如： 001 002 003
    """

    reference_file = fetch_files_by_name(reference_file).pop()

    if comparison_files:
        comparison_files = fetch_files_by_name(comparison_files)
    else:
        comparison_files = fetch_files_by_name('all')

    comparison_files = [comparison_file for comparison_file in comparison_files
                        if comparison_file.id != reference_file.id]

    physical_quantities = fetch_physical_quantities_by_name(physical_quantities)
    nuc_data_id = fetch_extracted_data_id([reference_file, *comparison_files],
                                          physical_quantities,
                                          config.get_nuclide_list(nuclide_list))

    save_comparison_result_to_excel(nuc_data_id=nuc_data_id,
                                    reference_file=reference_file,
                                    comparison_files=comparison_files,
                                    result_path=result_path,
                                    physical_quantities=physical_quantities,
                                    deviation_mode=deviation_mode,
                                    threshold=threshold,
                                    is_all_step=is_all_step)


@main_cli.command()
@click.argument('filenames',
                nargs=-1)
@click.option('--result_path', '-rp',
              'result_path',
              default=config.get_file_path('result_file_path'),
              type=click.Path(),
              help='输出文件路径，默认读取配置文件中的路径')
@click.option('--model_path', '-mp',
              'model_path',
              default=config.get_anomaly_detection_config('model_path'),
              type=click.Path(),
              help='模型文件路径，默认读取配置文件中的路径')
@click.option('--physical_quantities', '-pq',
              'physical_quantities',
              default=['isotope'],
              type=click.Choice(all_physical_quantity_list,
                                case_sensitive=False),
              multiple=True,
              help='物理量，默认为 isotope')
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
def detect(filenames,
           result_path,
           model_path,
           physical_quantities,
           is_all_step,
           merge):
    """
    使用 iforest 对数据进行异常检测，并导出异常的数据至工作簿(xlsx文件)

    参数为文件列表(默认为所有文件)
    """

    if filenames:
        filenames = fetch_files_by_name(filenames)
    else:
        filenames = fetch_files_by_name('all')

    physical_quantities = fetch_physical_quantities_by_name(physical_quantities)
    model_name = str(Path(model_path).parent.joinpath(Path(model_path).stem))

    save_prediction_to_exel(filenames=filenames,
                            result_path=result_path,
                            model_name=model_name,
                            physical_quantities=physical_quantities,
                            is_all_step=is_all_step,
                            merge=merge)


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
    main_cli(prog_name='nuctool')


main()
