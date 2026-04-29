.PHONY: install download build

install:
	pip install -e .

download:
	python build_lido_sqlite.py --download --output data/lido.db

build:
	python build_lido_sqlite.py --input data/lido-export.ttl.gz --output data/lido.db
