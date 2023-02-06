from setuptools import setup, find_packages

setup(
    name="xapi-db-load",
    version="0.1",
    packages=find_packages(),
    include_package_data=True,
    entry_points="""
        [console_scripts]
        xapi-db-load=xapi_db_load.main:load_db
    """,
    install_requires=[
        "click",
        "clickhouse-connect[numpy]",
        "psycopg2-binary",
        "pymongo[srv]",
        "requests",
    ],
    url="https://github.com/openedx/xapi-db-load",
    project_urls={
        "Code": "https://github.com/openedx/xapi-db-load",
        "Issue tracker": "https://github.com/openedx/xapi-db-load/issues",
    },
    license="AGPLv3",
    author="Open edX",
    description="Loads testing xAPI events into databases and LRSs",
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)
