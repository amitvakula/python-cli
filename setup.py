# coding: utf-8

"""
    Flywheel CLI
"""
import os
import sys

from setuptools import setup, find_packages
from setuptools.command.install import install

NAME = "flywheel-cli"

# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

INSTALL_REQUIRES = [
    'boto3>=1.9.0',
    'crayons>=0.2.0',
    'fs>=2.4.4',
    'flywheel-bids~=0.8.0',
    'flywheel_migration>=0.10.0',
    'flywheel-sdk>=8.0.0',
    'pyinstaller~=3.3.1',
    'tzlocal~=1.5.1',
]

DEV_REQUIRES = [
    'pycodestyle~=2.4.0',
    'pylint~=2.3.0',
    'pytest~=3.6.0',
    'pytest-cov~=2.5.0',
]

with open('flywheel_cli/VERSION') as f:
    version = f.read().strip()

setup(
    name=NAME,
    version=version,
    description="Flywheel Command Line Interface",
    author_email="support@flywheel.io",
    url="",
    keywords=["Flywheel", "flywheel", "CLI"],
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    extras_require={
        'dev': DEV_REQUIRES
    },
    packages=find_packages(),
    license="MIT",
    project_urls={
        'Documentation': 'https://docs.flywheel.io/display/EM/CLI+-+Installation'
    },
    entry_points = {
        'console_scripts': [
            'fw=flywheel_cli.main:main',
        ]
    }
)
