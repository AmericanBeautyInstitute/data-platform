help:
	cat Makefile

################################################################################

build:
	uv sync
	make reformat
	make lint
	make type_check
	make test

lint:
	uv run ruff check --fix .

reformat:
	uv run ruff format .

setup:
	uv sync
	uv run pre-commit install --install-hooks

test:
	uv run pytest -x --cov

type_check:
	uv run mypy tests --ignore-missing-import

################################################################################

.PHONY: \
	build \
	help \
	lint \
	reformat \
	setup \
	test \
	type_check
