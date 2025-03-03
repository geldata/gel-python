Documentation
=============

About
-----

This directory contains the source code for gel-python documentation
and build scripts. The documentation uses Sphinx and reStructuredText.

Building the documentation
--------------------------

Install Sphinx and other dependencies (i.e. theme) needed for the documentation.
From the `docs` directory, use `pip`:

```
$ pip install -r requirements.txt
```

Build the documentation like this:

```
$ make html
```

The built documentation will be placed in the `docs/_build` directory. Open
`docs/_build/index.html` to view the documentation.

Helpful documentation build commands
------------------------------------

Clean the documentation build:

```
$ make clean
```

Test and check the links found in the documentation:

```
$ make linkcheck
```