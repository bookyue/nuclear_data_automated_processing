import pathlib
import re

from setuptools import find_packages
from setuptools import setup

from nuc_data_tool import __version__


def read(filename):
    filename = pathlib.Path.joinpath(pathlib.Path(__file__).parent, filename)
    text_type = type(u"")
    with filename.open(mode="r", encoding='utf-8') as fd:
        return re.sub(text_type(r':[a-z]+:`~?(.*?)`'), text_type(r'``\1``'), fd.read())


setup(
    name="nuc-data-tool",
    version=__version__,
    url="https://github.com/bookyue/nuclear_data_automated_processing",
    license='LGPL',

    author="Kyle Yue",
    author_email="sevenbookyue@gmail.com",

    description="an automated processing tool for specific nuclear data",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",

    packages=find_packages(exclude=('tests',)),

    include_package_data=True,

    install_requires=["SQLAlchemy >= 1.4.0", "pandas", "toml",
                      "protobuf", "openpyxl", "click",
                      "psycopg2", "mysql-connector-python", "pycaret >= 2.3.0"],
    python_requires=">=3.8",

    entry_points={
        "console_scripts": [
            "nuctool=nuc_data_tool.__main__:main",
        ]
    },

    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.8',
    ],
)
