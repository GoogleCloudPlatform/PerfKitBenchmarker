from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup, find_packages

setup(
    url='https://github.com/GoogleCloudPlatform/PerfKitBenchmarker',
    version='0.6.0',
    license='Apache 2.0',
    packages=find_packages(),
    scripts=['pkb.py'],
    install_requires=['python-gflags==2.0',
                      'jinja2>=2.7',
                      'oauth2client',
                      'pycrypto',
                      'pyOpenSSL'])
