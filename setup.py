"""Setup for the twikwak17 package."""

# !/usr/bin/env python
# -*- coding: utf-8 -*-

import setuptools
import versioneer


INSTALL_REQUIRES = [
    'psutil',
    'click',
    'birch>=0.0.13',
    'sortedcontainers>=2.0',
    'ezenum>=0.0.3',
    'speks',
]
TEST_REQUIRES = [
    # testing and coverage
    'pytest', 'coverage', 'pytest-cov',
    # to be able to run `python setup.py checkdocs`
    'collective.checkdocs', 'pygments',
]

with open('README.rst') as f:
    README = f.read()

setuptools.setup(
    author="Shay Palachy",
    author_email="shay.palachy@gmail.com",
    name='twikwak17',
    license="MIT",
    description='A gender-augmented Twitter network dataset.',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    long_description=README,
    url='https://github.com/shaypal5/twikwak17',
    packages=setuptools.find_packages(),
    include_package_data=True,
    python_requires=">=3.5",
    install_requires=INSTALL_REQUIRES,
    extras_require={
        'test': TEST_REQUIRES + INSTALL_REQUIRES,
    },
    entry_points='''
        [console_scripts]
        twikwak=scripts.twikwak_cli:cli
    ''',
    classifiers=[
        # Trove classifiers
        # (https://pypi.python.org/pypi?%3Aaction=list_classifiers)
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Intended Audience :: Developers',
    ],
)
