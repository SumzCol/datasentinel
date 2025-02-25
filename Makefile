install-uv:
	python -m pip install "uv==0.6.2"

install-base-reqs: install-uv
	uv pip install --system "dataguard @ ."

install-lint-reqs: install-uv
	uv pip install --system "dataguard[lint] @ ."

install-test-reqs: install-uv
	uv pip install --system "dataguard[test] @ ."

install-scripts-reqs: install-uv
	uv pip install --system "dataguard[scripts] @ ."

install-pre-commit:
	pre-commit install --install-hooks

uninstall-pre-commit:
	pre-commit uninstall

build-package:
	python -m pip install build && python -m build --wheel

lint:
	pre-commit run --all-files --hook-stage manual

typecheck:
	mypy dataguard

unit-test:
	pytest -m unit

functional-test:
	pytest -m functional
