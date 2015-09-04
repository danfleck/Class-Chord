import ez_setup
ez_setup.use_setuptools()

from setuptools import setup, find_packages
import os

packageDir = os.path.join("network-client", "src")
setup(
    name = "GMU-ClassChord-Networking",
    version = "0.4",
    packages = find_packages(packageDir, exclude=["tests"]),
    package_dir = {'':packageDir},   # tell distutils packages are under src

    # Installation requirements. These are PyPi project names. 
    install_requires = ['twisted>=13.2.0', 'miniupnpc'],

    package_data = {
        # If any package contains *.txt or *.rst files, include them:
        '': ['*.txt']
    },
    zip_safe=True,

    test_suite='tests',
    # metadata for upload to PyPI
    author = "George Mason University, Dan Fleck",
    author_email = "dfleck@gmu.edu",
    description = "This package contains a full working implementation of the Class-Chord algorithm.",
)
