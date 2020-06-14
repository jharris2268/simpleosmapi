#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='simpleosmapi',
      version='0.1',
      description='simpleosmapi',
      author='James Harris',

      url='https://www.github.com/jharris2268/osmutils',
      packages = find_packages(),
      scripts=['simpleosmapi_server.py',],
      include_package_data=True,
      zip_safe=False
)
