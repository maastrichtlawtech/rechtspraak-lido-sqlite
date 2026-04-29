.PHONY: install install-tools download build

install:
	pip install -e .

install-tools:
	brew install serd   # provides serdi (TTL→N-Triples converter, lax mode)

download:
	python build_lido_sqlite.py --download --output data/lido.db

build:
	python build_lido_sqlite.py --input data/lido-export.ttl.gz --output data/lido.db
