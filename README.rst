Scripts for generating and loading test xAPI events
===================================================

Some test scripts to help make apples-to-apples comparisons of different
database backends for xAPI events. Supports direct database connections to
ClickHouse, MongoDB, Citus PostgreSQL, and batch loading data to the Ralph
Learning Record Store with ClickHouse or MongoDB backends.

xAPI events generated match the specifications of the Open edX
event-routing-backends package, but are not yet maintained to advance alongside
them.

Installation
------------

PyPI package coming soon, for now you can clone the repo and install via:

::

    ❯ pip install .


Usage
-----

Details of how to run the current version of the script can be found by executing:

::

    ❯ xapi-db-load --help


License
-------

This software is licensed under the terms of the AGPLv3.
