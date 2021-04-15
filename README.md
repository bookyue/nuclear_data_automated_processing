# Nuclear Data Automated Processing

An automated processing tool for specific nuclear data.

## Installation

You can install the Nuclear Data Automated Processing tool from PyPI:

```bash
pip install nuc-data-tool
```

The tool is supported on Python 3.8 and above.  
And it is supported on postgresql(>= 13), mysql(>= 8.0)

## How to use

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
  pop      将输入文件(*.xml.out) 的内容填充进数据库
```


You can also call the Nuclear Data Automated Processing tool in your own Python code, by importing from the `nuc_data_tool` package:

```python
>>> from nuc_data_tool.db.fetch import fetch_files_by_name
>>> print(fetch_files_by_name())
...
```