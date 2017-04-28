import re

from pipenv.project import Project
from pipenv.utils import convert_deps_to_pip
from setuptools import find_packages, setup


with open('LICENSE') as f:
    license = f.read()

pipfile = Project().parsed_pipfile
requirements = convert_deps_to_pip(pipfile['packages'], r=False)

with open('pit/__init__.py') as f:
        version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                            f.read(), re.MULTILINE).group(1)


setup(
    name='pit',
    version=version,
    license=license,
    author='Mike Graves',
    author_email='mgraves@mit.edu',
    packages=find_packages(exclude=['tests']),
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'pit = pit.cli:main',
        ]
    },
)
