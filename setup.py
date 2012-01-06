#!/usr/bin/env python
import os
from setuptools import setup

from beanstalkc import __version__ as src_version

pkg_version = os.environ.get('BEANSTALKC_PKG_VERSION', src_version)

setup(
    name='beanstalkc-timeout',
    version=pkg_version,
    py_modules=['beanstalkc_timeout'],

    author='Michael Schurter',
    author_email='m@schmichael.com',
    description='A beanstalkd client library with socket timeout support',
    long_description='''
beanstalkc is a beanstalkd client library for Python. beanstalkc-timeout is a
slight fork to support socket timeouts and SO_KEEPALIVE for suboptimal network
configurations. `beanstalkd <http://kr.github.com/beanstalkd/>`_ is a fast,
distributed, in-memory workqueue service.
''',
    url='http://github.com/schmichael/beanstalkc-timeout',
    license='Apache License, Version 2.0',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
