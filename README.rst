Scripts for generating and loading test xAPI events
===================================================

|pypi-badge| |ci-badge| |codecov-badge| |doc-badge| |pyversions-badge|
|license-badge| |status-badge|


Purpose
*******

Some test scripts to help make apples-to-apples comparisons of different
database backends for xAPI events. Supports direct database connections to
ClickHouse, MongoDB, Citus PostgreSQL, and batch loading data to the Ralph
Learning Record Store with ClickHouse or MongoDB backends.

xAPI events generated match the specifications of the Open edX
event-routing-backends package, but are not yet maintained to advance alongside
them.

Getting Started
***************

Usage
=====

Details of how to run the current version of the script can be found by executing:

::

    ❯ xapi-db-load --help


Developing
==========

One Time Setup
--------------
.. code-block::

  # Clone the repository
  git clone git@github.com:openedx/xapi-db-load.git
  cd xapi-db-load

  # Set up a virtualenv using virtualenvwrapper with the same name as the repo and activate it
  mkvirtualenv -p python3.8 xapi-db-load


Every time you develop something in this repo
---------------------------------------------
.. code-block::

  # Activate the virtualenv
  workon xapi-db-load

  # Grab the latest code
  git checkout main
  git pull

  # Install/update the dev requirements
  make requirements

  # Run the tests and quality checks (to verify the status before you make any changes)
  make validate

  # Make a new branch for your changes
  git checkout -b <your_github_username>/<short_description>

  # Using your favorite editor, edit the code to make your change.
  vim ...

  # Run your new tests
  pytest ./path/to/new/tests

  # Run all the tests and quality checks
  make validate

  # Commit all your changes
  git commit ...
  git push

  # Open a PR and ask for review.


Getting Help
************

Documentation
=============

Start by going through `the documentation`_ (in progress!).

.. _the documentation: https://docs.openedx.org/projects/xapi-db-load


More Help
=========

If you're having trouble, we have discussion forums at
https://discuss.openedx.org where you can connect with others in the
community.

Our real-time conversations are on Slack. You can request a `Slack
invitation`_, then join our `community Slack workspace`_.

For anything non-trivial, the best path is to open an issue in this
repository with as many details about the issue you are facing as you
can provide.

https://github.com/openedx/xapi-db-load/issues

For more information about these options, see the `Getting Help`_ page.

.. _Slack invitation: https://openedx.org/slack
.. _community Slack workspace: https://openedx.slack.com/
.. _Getting Help: https://openedx.org/getting-help

License
*******

The code in this repository is licensed under the AGPL 3.0 unless
otherwise noted.

Please see `LICENSE.txt <LICENSE.txt>`_ for details.

Contributing
************

Contributions are very welcome.
Please read `How To Contribute <https://openedx.org/r/how-to-contribute>`_ for details.

This project is currently accepting all types of contributions, bug fixes,
security fixes, maintenance work, or new features.  However, please make sure
to have a discussion about your new feature idea with the maintainers prior to
beginning development to maximize the chances of your change being accepted.
You can start a conversation by creating a new issue on this repo summarizing
your idea.

The Open edX Code of Conduct
****************************

All community members are expected to follow the `Open edX Code of Conduct`_.

.. _Open edX Code of Conduct: https://openedx.org/code-of-conduct/

People
******

The assigned maintainers for this component and other project details may be
found in `Backstage`_. Backstage pulls this data from the ``catalog-info.yaml``
file in this repo.

.. _Backstage: https://open-edx-backstage.herokuapp.com/catalog/default/component/xapi-db-load

Reporting Security Issues
*************************

Please do not report security issues in public. Please email security@tcril.org.

.. |pypi-badge| image:: https://img.shields.io/pypi/v/xapi-db-load.svg
    :target: https://pypi.python.org/pypi/xapi-db-load/
    :alt: PyPI

.. |ci-badge| image:: https://github.com/openedx/xapi-db-load/workflows/Python%20CI/badge.svg?branch=main
    :target: https://github.com/openedx/xapi-db-load/actions
    :alt: CI

.. |codecov-badge| image:: https://codecov.io/github/openedx/xapi-db-load/coverage.svg?branch=main
    :target: https://codecov.io/github/openedx/xapi-db-load?branch=main
    :alt: Codecov

.. |doc-badge| image:: https://readthedocs.org/projects/xapi-db-load/badge/?version=latest
    :target: https://xapi-db-load.readthedocs.io/en/latest/
    :alt: Documentation

.. |pyversions-badge| image:: https://img.shields.io/pypi/pyversions/xapi-db-load.svg
    :target: https://pypi.python.org/pypi/xapi-db-load/
    :alt: Supported Python versions

.. |license-badge| image:: https://img.shields.io/github/license/openedx/xapi-db-load.svg
    :target: https://github.com/openedx/xapi-db-load/blob/main/LICENSE.txt
    :alt: License

.. |status-badge| image:: https://img.shields.io/badge/Status-Experimental-yellow
