install-uv:
	python -m pip install "uv==0.6.2"

install: install-uv
	uv pip install --system "dataguard @ ."

install-lint: install-uv
	uv pip install --system "dataguard[lint] @ ."

install-test: install-uv
	uv pip install --system "dataguard[test] @ ."

install-scripts: install-uv
	uv pip install --system "dataguard[scripts] @ ."

install-pre-commit:
	pre-commit install --install-hooks

install-all: install install-lint install-test install-pre-commit

uninstall-pre-commit:
	pre-commit uninstall

build-package:
	python -m pip install build && python -m build --wheel

lint:
	pre-commit run --all-files --hook-stage manual

type-check:
	pyright dataguard

unit-test:
	pytest -m unit

functional-test:
	pytest -m functional
