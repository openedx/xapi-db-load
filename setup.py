"""
Setup for xapi-db-load.
"""
from setuptools import find_packages, setup

from xapi_db_load import __version__

with open("README.rst", "r") as readme:
    desc = readme.read()

setup(
    name="xapi-db-load",
    version=__version__,
    packages=find_packages(),
    include_package_data=True,
    entry_points="""
        [console_scripts]
        xapi-db-load=xapi_db_load.main:cli
    """,
    install_requires=[
        "click",
        "clickhouse-connect >= 0.5, < 0.7",
        "requests",
        "smart_open[s3]",
    ],
    url="https://github.com/openedx/xapi-db-load",
    project_urls={
        "Code": "https://github.com/openedx/xapi-db-load",
        "Issue tracker": "https://github.com/openedx/xapi-db-load/issues",
    },
    license="AGPLv3",
    author="Open edX",
    description="Loads testing xAPI events into databases and LRSs",
    long_description=desc,
    long_description_content_type="text/x-rst",
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
