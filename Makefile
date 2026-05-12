.PHONY: install install-tools download build

install:
	pip install -e .

install-tools:
	@if [ "$$(uname -s)" = "Darwin" ]; then \
		brew install serd; \
	elif [ "$$(uname -s)" = "Linux" ]; then \
		sudo apt-get update && sudo apt-get install -y serdi; \
	else \
		echo "Unsupported OS: $$(uname -s)"; \
		exit 1; \
	fi

download:
	python build_lido_sqlite.py --download --output data/lido.db

build:
	python build_lido_sqlite.py --input data/lido-export.ttl.gz --output data/lido.db
