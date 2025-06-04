.PHONY: compile debug test quicktest clean all gen-errors gen-types _touch


PYTHON ?= python
ROOT = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))


all: compile


clean:
	rm -fr $(ROOT)/dist/
	rm -fr $(ROOT)/doc/_build/
	rm -fr $(ROOT)/gel/pgproto/*.c
	rm -fr $(ROOT)/gel/pgproto/*.html
	rm -fr $(ROOT)/gel/pgproto/codecs/*.html
	rm -fr $(ROOT)/gel/protocol/*.c
	rm -fr $(ROOT)/gel/protocol/*.html
	rm -fr $(ROOT)/gel/protocol/*.so
	rm -fr $(ROOT)/gel/datatypes/*.so
	rm -fr $(ROOT)/gel/datatypes/datatypes.c
	rm -fr $(ROOT)/build
	rm -fr $(ROOT)/gel/protocol/codecs/*.html
	find . -name '__pycache__' | xargs rm -rf


_touch:
	rm -fr $(ROOT)/gel/datatypes/datatypes.c
	rm -fr $(ROOT)/gel/protocol/protocol.c
	find $(ROOT)/gel/protocol -name '*.pyx' | xargs touch
	find $(ROOT)/gel/datatypes -name '*.pyx' | xargs touch
	find $(ROOT)/gel/datatypes -name '*.c' | xargs touch


compile: _touch
	$(PYTHON) setup.py build_ext --inplace


compile-fast:
	rm -fr $(ROOT)/gel/datatypes/datatypes.c
	rm -fr $(ROOT)/gel/protocol/protocol.c
	$(PYTHON) setup.py build_ext --inplace


gen-errors:
	edb gen-errors --import "$$(printf 'from gel.errors._base import *\nfrom gel.errors.tags import *')" \
		--extra-all "_base.__all__" --stdout --client > $(ROOT)/.errors
	mv $(ROOT)/.errors $(ROOT)/gel/errors/__init__.py
	$(PYTHON) tools/gen_init.py


gen-types:
	edb gen-types --stdout > $(ROOT)/gel/protocol/codecs/edb_types.pxi


debug: _touch
	EDGEDB_DEBUG=1 $(PYTHON) setup.py build_ext --inplace


test:
	PYTHONASYNCIODEBUG=1 $(PYTHON) -m unittest tests.suite
	$(PYTHON) -m unittest tests.suite
	USE_UVLOOP=1 $(PYTHON) -m unittest tests.suite


testinstalled:
	cd /tmp && $(PYTHON) $(ROOT)/tests/__init__.py


quicktest:
	$(PYTHON) -m unittest tests.suite


htmldocs:
	$(PYTHON) setup.py build_ext --inplace
	$(MAKE) -C docs html SPHINXOPTS="-W -n"
