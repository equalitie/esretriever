from __future__ import absolute_import
from setuptools import setup
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# README as the long description
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

REQUIREMENTS = [i.strip() for i in open('requirements.txt').readlines()]
tests_require = [
          'spark-testing-base',
          'pytest',
          'mock',
          'undecorated'
      ]

setup(name='es_retriever',
      version='0.1.3',
      description='Retrieve Logs from ES with Spark',
      long_description=long_description,
      tests_require=tests_require,
      extras_require={
        'test': tests_require
      },
      test_suite='pytest.collector',
      install_requires=REQUIREMENTS,
      package_dir={'': 'src'},
      packages=[
          'es_retriever',
          'es_retriever.es',
          'es_retriever.helpers',
          'es_retriever.spark',
        ],
      entry_points={'console_scripts': ['esretrieve=es_retriever.cli:main']}
      )
