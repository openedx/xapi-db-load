Scripts for load testing xAPI events into databases
===================================================================================

Just some test scripts to help make apples-to-apples comparisons of different
database backends for xAPI events. Currently only supports Clickhouse.

Installation
------------

Requires a running Clickhouse database to connect to, you will be prompted for the
password. Just clone the repo, install the requirements, and go. Defaults given are
for a default install of the Tutor Clickhouse plugin:
https://github.com/bmtcril/tutor-contrib-clickhouse

To get the password from a Tutor Clickhouse plugin, just run the following:

`tutor config printvalue CLICKHOUSE_LRS_PASSWORD`


Usage
-----

::

    ❯ pip install .
    ❯ xapi-db-load --help
    Usage: main.py [OPTIONS]

    Options:
  --backend [clickhouse|mongo|citus|ralph_clickhouse|ralph_mongo]
                                  Which backend to run against  [required]
  --num_batches INTEGER           Number of batches to run, num_batches *
                                  batch_size is the total rows
  --batch_size INTEGER            Number of rows to insert per batch,
                                  num_batches * batch_size is the total rows
  --drop_tables_first BOOLEAN     If True, the target tables will be dropped
                                  if they already exist
  --distributions_only BOOLEAN    Just run distribution queries and exit
  --db_host TEXT                  Database host name
  --db_port TEXT                  Database port
  --db_name TEXT                  Database name
  --db_username TEXT              Database username
  --db_password TEXT              Password for the database so it's not stored
                                  on disk
  --lrs_url TEXT                  URL to the LRS, if used
  --lrs_username TEXT             LRS username
  --lrs_password TEXT             Password for the LRS so it's not stored on
                                  disk
  --help                          Show this message and exit.
      --backend [clickhouse|mongo|citus]
                                   Which database backend to run against
                                   [required]
      --num_batches INTEGER        Number of batches to run, num_batches *
                                   batch_size is the total rows
      --batch_size INTEGER         Number of rows to insert per batch, num_batches
                                   * batch_size is the total rows
      --drop_tables_first BOOLEAN  If True, the target tables will be dropped if
                                   they already exist
      --host TEXT                  Database host name
      --port TEXT                  Database port
      --username TEXT              Database username
      --password TEXT              Password for the database so it's not stored on
                                   disk
      --database TEXT              Database name
      --help                       Show this message and exit.




License
-------

This software is licensed under the terms of the AGPLv3.
