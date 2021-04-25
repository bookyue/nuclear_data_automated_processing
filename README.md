# Nuclear Data Automated Processing

An automated processing tool for specific nuclear data.

## Demo
[![asciicast](https://asciinema.org/a/NgX9tzRrKbCgTe8XDmYE6xG8N.svg)](https://asciinema.org/a/NgX9tzRrKbCgTe8XDmYE6xG8N)

## Installation
### Dependencies
nuc_tool requires:

* SQLAlchemy (>= 1.4.7)  
* pandas (>= 1.2.4)  
* toml (>= 0.10.2)  
* protobuf (>= 3.15.8)  
* openpyxl (>= 3.0.7)  
* click (>= 7.1.2)  
* psycopg2-binary (>= 2.8.6)  
* mysql-connector-python (>= 8.0.23)  

***

### Set up a database
nuc-tool supports 3 different database servers and is officially supported with PostgreSQL, MySQL, and SQLite.  
If you’re using PostgreSQL, you’ll need the psycopg2 package.  
If you’re using MySQL or MariaDB, you’ll need a DB API driver like mysql-connector-python.  
Recommend using docker container to run database servers.  
You can refer these two example `docker-compose` [files](https://github.com/bookyue/nuclear_data_automated_processing/tree/main/database).

### Install nuc-tool

You can install the Nuclear Data Automated Processing tool from PyPI:

```bash
pip install nuc-data-tool
```

The tool is supported on Python 3.8 and above.  
And it is supported on postgresql(>= 13), mysql(>= 8.0)

## How to use

The `config.toml`(config file) is distributed with package.  
You can copy or download the [configration file](https://raw.githubusercontent.com/bookyue/nuclear_data_automated_processing/main/nuc_data_tool/config.toml)
into the current working directory.  
it will overide the default configration.

The Nuclear Data Automated Processing tool is a command line application, named `nuctool`

```bash
❯ python -m nuc_data_tool
Usage: python -m nuc_data_tool [OPTIONS] COMMAND [ARGS]...

  app 命令行

Options:
  --version  Show the version and exit.
  --help     Show this message and exit.

Commands:
  compare  对文件列表进行两两组合，进行对比，计算并输出对比结果至工作簿(xlsx文件)
  extract  从数据库导出选中的文件的数据到工作簿(xlsx文件)
  fetch    获取 文件、物理量信息
  pop      将输出文件(*.xml.out) 的内容填充进数据库
```

### Populate database
```bash
> nuctool pop --help
Usage: python -m nuc_data_tool pop [OPTIONS]

  将输出文件(*.xml.out) 的内容填充进数据库

Options:
  -p, --path PATH                 输出文件路径，默认读取配置文件中的路径
  -pq, --physical_quantities [isotope|radioactivity|absorption|fission|decay_heat|gamma_spectra]
                                  物理量，默认为全部物理量
  -init, --initiation             初始化数据库
  --help                          Show this message and exit.
```

The first step has to get those data into the database.  
We can specify the data file location with option `-p, --path`.  
The option `-pq, --physical_quantities` can be input multiple times. So we could focus on the physical quantities we interest in.  
Only those physical quantities will be get into the database.  
If you want a "fresh new" database, you should append the `-init, --initiation` option.  
It will drop all tables, of course, including data, and then create all tables.  

```bash
> nuctool pop -p input_file -pq isotope -pq gamma_spectra -init
UO2Flux_CRAM_1ton_100steps:
found:     ['isotope']
not found: ['gamma_spectra']

homo-case001-006:
found:     ['isotope', 'gamma_spectra']
not found: []
```

### View files and physical quantities
```bash
> nuctool fetch --help
Usage: python -m nuc_data_tool fetch [OPTIONS]

  获取 文件、物理量信息

Options:
  -f, --file               显示文件信息 NOTE: This argument is mutually exclusive
                           with  arguments: [physical_quantity].

  -p, --physical_quantity  显示物理量信息 NOTE: This argument is mutually exclusive
                           with  arguments: [files].

  -l, --list               以数组形式输出
  --help                   Show this message and exit.
```

This command is greatly straightforward.  
The `-f, --file` option will list the file names.  
The `-p, --physical_quantity` option will list the physical quantity names.  
Attention, they are mutually exclusive to each other.  

The last one, `-l, --list ` option could be a combination with one of the above two options.  
It affects the display in an array-like format.  

```bash
> nuctool fetch -f
Name: UO2Flux_CRAM_1ton_100steps
Name: UO2Flux_CRAM_1ton_50steps
Name: homo-case001-006
...

> nuctool fetch -f -l
['UO2Flux_CRAM_1ton_100steps', ... 'homo-case001-006']

> nuctool fetch -p
Name: isotope
Name: gamma_spectra

> nuctool fetch -p -l
['isotope', 'gamma_spectra']
```

### Filter and extract data
```bash
> nuctool extract --help
Usage: python -m nuc_data_tool extract [OPTIONS]

  从数据库导出选中的文件的数据到工作簿(xlsx文件)

Options:
  -f, --files TEXT                文件名(列表)(没有后缀) 例如：001.xml.out -> 001，默认为所有文件
                                  例子： 003  001,002  '[001,003]'

  -p, --result_path PATH          输出文件路径，默认读取配置文件中的路径
  -pq, --physical_quantities [isotope|radioactivity|absorption|fission|decay_heat|gamma_spectra]
                                  物理量，默认为全部物理量
  -n, --nuclide [decay|fission_light|short_lives]
                                  核素列表，从配置文件 nuclide_list 项下读取，默认
                                  fission_light

  -all, --all_step                提取中间步骤
  -m, --merge                     将结果合并输出至一个文件
  --help                          Show this message and exit.
```

We can specify the exported file location with option `-p, --result_path`.  
The option `-pq, --physical_quantities` can be input multiple times. So we could focus on the physical quantities we interest in.  
Only those physical quantities will be extracted with data.

```
> nuctool extract -f homo-case097-102,homo-case139-144 -p result

> ls result
homo-case097-102.xlsx  homo-case139-144.xlsx
```

We filter data by nuclide list with `-n, --nuclide` option. You can find these details in the `config.toml` file.

Extracting all steps by using `-all, --all_step ` option, if not, it just extracts the first step and the last step.

```bash
> nuctool extract -f homo-case097-102,homo-case139-144 -p result -all

> ls result
all_steps_homo-case097-102.xlsx  all_steps_homo-case139-144.xlsx
```

Appending the `-m, --merge` option, the extracted data will merge into a single file, instead of one by one file.

```bash
> nuctool extract -f homo-case097-102,homo-case139-144 -p result -m
> nuctool extract -f homo-case097-102,homo-case139-144 -p result -all -m
> ls result
all_steps_final.xlsx  final.xlsx
```

### Compare and extract data
```bash
> nuctool compare --help
Usage: python -m nuc_data_tool compare [OPTIONS]

  对文件列表进行两两组合，进行对比，计算并输出对比结果至工作簿(xlsx文件)

Options:
  -f, --files TEXT                文件名(列表)(没有后缀) 例如：001.xml.out -> 001，默认为所有文件
                                  例子： 003  001,002,003  '[001,004,003]'

  -p, --result_path PATH          输出文件路径，默认读取配置文件中的路径
  -pq, --physical_quantities [isotope|radioactivity|absorption|fission|decay_heat|gamma_spectra]
                                  物理量，默认为 isotope
  -n, --nuclide [decay|fission_light|short_lives]
                                  核素列表，从配置文件 nuclide_list 项下读取
  -dm, --deviation_mode [relative|absolute]
                                  偏差模式，分为绝对和相对，默认为相对
  -t, --threshold TEXT            偏差阈值，默认1.0E-12
  -all, --all_step                提取中间步骤
  --help                          Show this message and exit.
```

The last but not least command, we used to compare and extract data.  

We can specify the exported file location with option `-p, --result_path`.  
The option `-pq, --physical_quantities` can be input multiple times. So we could focus on the physical quantities we interest in.  
Only those physical quantities will be campared and extracted.  

The `-f, --files` option has a little complex.  
Let's talk about a few details here.  
This option takes array-like input, e.g. 001,002,003  '[001,004,003]'.  
Then it generates all possible combinations of length 2 from the input.  
Finally, compare these combinations one by one.  

```bash
> nuctool compare -f 'UO2Flux_CRAM_1ton_50steps','homo-case007-012','homo-case013-018' -p result
('UO2Flux_CRAM_1ton_50steps', 'homo-case007-012')
('UO2Flux_CRAM_1ton_50steps', 'homo-case013-018')
('homo-case007-012', 'homo-case013-018')

> ls result/comparative_result
relative_1.0E-12_homo-case007-012_vs_homo-case013-018.xlsx
relative_1.0E-12_UO2Flux_CRAM_1ton_50steps_vs_homo-case007-012.xlsx
relative_1.0E-12_UO2Flux_CRAM_1ton_50steps_vs_homo-case013-018.xlsx
```

Like the former one, we filter data by nuclide list with `-n, --nuclide` option. You can find these details in the `config.toml` file.

Same, extracting all steps by using `-all, --all_step ` option, if not, it just extracts the first step and the last step.

```bash
> nuctool compare -f 'UO2Flux_CRAM_1ton_50steps','homo-case007-012','homo-case013-018' -p result -all
('UO2Flux_CRAM_1ton_50steps', 'homo-case007-012')
('UO2Flux_CRAM_1ton_50steps', 'homo-case013-018')
('homo-case007-012', 'homo-case013-018')

> ls result/comparative_result
all_step_relative_1.0E-12_homo-case007-012_vs_homo-case013-018.xlsx
all_step_relative_1.0E-12_UO2Flux_CRAM_1ton_50steps_vs_homo-case007-012.xlsx
all_step_relative_1.0E-12_UO2Flux_CRAM_1ton_50steps_vs_homo-case013-018.xlsx
```

There are two deviation mode of calculation. The one is relative mode, the other one is absolute mode.  
We use `-dm, --deviation_mode` option to choose one of them.  

```bash
> nuctool compare -f 'UO2Flux_CRAM_1ton_50steps','homo-case007-012','homo-case013-018' -p result -dm absolute
('UO2Flux_CRAM_1ton_50steps', 'homo-case007-012')
('UO2Flux_CRAM_1ton_50steps', 'homo-case013-018')
('homo-case007-012', 'homo-case013-018')

> ls result/comparative_result
absolute_1.0E-12_homo-case007-012_vs_homo-case013-018.xlsx
absolute_1.0E-12_UO2Flux_CRAM_1ton_50steps_vs_homo-case007-012.xlsx
absolute_1.0E-12_UO2Flux_CRAM_1ton_50steps_vs_homo-case013-018.xlsx
```

The `-t, --threshold` option defines the threshold value of calculation.  
The default value is `1.0E-12`. I think maybe not touching it is a good idea.

```bash
> nuctool compare -f 'UO2Flux_CRAM_1ton_50steps','homo-case007-012','homo-case013-018' -p result -dm absolute -t 1.0E-10
('UO2Flux_CRAM_1ton_50steps', 'homo-case007-012')
('UO2Flux_CRAM_1ton_50steps', 'homo-case013-018')
('homo-case007-012', 'homo-case013-018')

> ls result/comparative_result
absolute_1.0E-10_homo-case007-012_vs_homo-case013-018.xlsx
absolute_1.0E-10_UO2Flux_CRAM_1ton_50steps_vs_homo-case007-012.xlsx
absolute_1.0E-10_UO2Flux_CRAM_1ton_50steps_vs_homo-case013-018.xlsx
```

### Import as a package.

You can also call the Nuclear Data Automated Processing tool in your own Python code, by importing from the `nuc_data_tool` package:

```python
>>> from nuc_data_tool.db.fetch_data import fetch_files_by_name
>>> for file in fetch_files_by_name('all'):
...     print(file.name)
... 
UO2Flux_CRAM_1ton_100steps
UO2Flux_CRAM_1ton_10steps
UO2Flux_CRAM_1ton_50steps
homo-case001-006
homo-case007-012
homo-case013-018
...
```
