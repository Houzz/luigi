# Copyright (c) 2012 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import os
import sys

try:
    from setuptools import setup
    from setuptools.command.test import test as TestCommand
except:
    from distutils.core import setup
    from distutils.cmd import Command as TestCommand


class Tox(TestCommand):
    user_options = [('tox-args=', None, "Arguments to pass to tox")]
    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.tox_args = ''
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True
    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import tox
        errno = tox.cmdline(args=self.tox_args.split())
        sys.exit(errno)


def get_static_files(path):
    return [os.path.join(dirpath.replace("luigi/", ""), ext) 
            for (dirpath, dirnames, filenames) in os.walk(path)
            for ext in ["*.html", "*.js", "*.css", "*.png"]]


luigi_package_data = sum(map(get_static_files, ["luigi/static", "luigi/templates"]), [])


long_description = ['Note: For the latest source, discussion, etc, please visit the `Github repository <https://github.com/spotify/luigi>`_\n\n']
for line in open('README.rst'):
    long_description.append(line)
long_description = ''.join(long_description)


setup(
    name='luigi',
    version='1.0.19',
    description='Workflow mgmgt + task scheduling + dependency resolution',
    long_description=long_description,
    author='Erik Bernhardsson',
    author_email='erikbern@spotify.com',
    url='https://github.com/spotify/luigi',
    license='Apache License 2.0',
    install_requires = ['PyMySQL>=0.5', 'tblib'],
    packages=[
        'luigi',
        'luigi.contrib',
        'luigi.tools'
    ],
    package_data={
        'luigi': luigi_package_data
    },
    scripts=[
        'bin/luigid',
        'bin/luigi'
    ],
    tests_require=['tox', 'virtualenv'],
    cmdclass={'test': Tox},
)
