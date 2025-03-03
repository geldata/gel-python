The Python driver for Gel
=========================

.. image:: https://github.com/geldata/gel-python/workflows/Tests/badge.svg?event=push&branch=master
    :target: https://github.com/geldata/gel-python/actions

.. image:: https://img.shields.io/pypi/v/gel.svg
    :target: https://pypi.python.org/pypi/gel

.. image:: https://img.shields.io/badge/join-github%20discussions-green
    :target: https://github.com/geldata/gel/discussions


**gel-python** is the official Gel driver for Python.
It provides both blocking IO and asyncio implementations.

The library requires Python 3.8 or later.


Documentation
-------------

The project documentation can be found
`here <https://docs.geldata.com/reference/clients/python#gel-python-intro>`_.


Installation
------------

The library is available on PyPI.  Use ``pip`` to install it::

    $ pip install gel


Basic Usage
-----------

.. code-block:: python

    import datetime
    import gel

    def main():
        client = gel.create_client()
        # Create a User object type
        client.execute('''
            CREATE TYPE User {
                CREATE REQUIRED PROPERTY name -> str;
                CREATE PROPERTY dob -> cal::local_date;
            }
        ''')

        # Insert a new User object
        client.query('''
            INSERT User {
                name := <str>$name,
                dob := <cal::local_date>$dob
            }
        ''', name='Bob', dob=datetime.date(1984, 3, 1))

        # Select User objects.
        user_set = client.query(
            'SELECT User {name, dob} FILTER .name = <str>$name', name='Bob')
        # *user_set* now contains
        # Set{Object{name := 'Bob', dob := datetime.date(1984, 3, 1)}}

        # Close the client.
        client.close()

    if __name__ == '__main__':
        main()

Development
-----------

Instructions for installing Gel and gel-python locally can be found at
`docs.geldata.com/resources/guides/contributing/code <https://docs.geldata.com/resources/guides/contributing/code>`_.

To run the test suite, run ``$ python setup.py test``.

License
-------

gel-python is developed and distributed under the Apache 2.0 license.
