#!/usr/bin/env python3

# Update dependencies:
#
#  - python2 -m genomic-benchmark venv create
#  - venv/cpython2<tab>/bin/python -m pip list --outdated
#  - update genomic-benchmark/requirements.txt
#  - (see also pip-tools and pipdeptree tools)
#  - (pip install pip-review; pip-review --local --interactive)
#
# Prepare a release:
#
#  - git pull --rebase
#  - Remove untracked files/dirs: git clean -fdx
#  - maybe update version in setup.py, genomic-benchmark/__init__.py and doc/conf.py
#  - set release date in doc/changelog.rst
#  - git commit -a -m "prepare release x.y"
#  - run tests: tox
#  - git push
#  - check Travis CI status:
#    https://travis-ci.org/python/genomic-benchmark
#
# Release a new version:
#
#  - git tag VERSION
#  - git push --tags
#  - Remove untracked files/dirs: git clean -fdx
#  - python3 setup.py sdist bdist_wheel
#  - python2 setup.py bdist_wheel
#  - twine upload dist/*
#
# After the release:
#
#  - set version to n+1: setup.py, genomic-benchmark/__init__.py and doc/conf.py
#  - git commit -a -m "post-release"
#  - git push

from setuptools import setup, find_packages

VERSION = '0.1.0'

DESCRIPTION = 'Python genomic benchmark suite'

CLASSIFIERS = [
    'Development Status :: 2 - Pre-Alpha',
    'Intended Audience :: Science/Research',
    'License :: OSI Approved :: MIT License',
    'Natural Language :: English',
    'Operating System :: OS Independent',
    'Environment :: Console',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python',
]

with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name='genomics-benchmarks',
    version=VERSION,
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/ornl-oxford/genomics-benchmarks/',
    license='MIT',
    author='ornl-oxford',
    packages=find_packages(),
    include_package_data=True,
    package_data={
        'genomics_benchmarks': ['config/benchmark.conf.default']
    },
    classifiers=CLASSIFIERS,
    install_requires=[
        'matplotlib',
        'numpy',
        'pandas',
        'scipy',
        'dask',
        'numcodecs',
        'zarr',
        'scikit-allel',
        'perf',
        'toolz'
    ],
    entry_points={
        'console_scripts': [
            'genomics-benchmarks = genomics_benchmarks:main'
        ]
    }
)
