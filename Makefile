help:
	cat Makefile

################################################################################

build:
	uv sync
	make reformat
	make lint
	make type_check
	make docs
	make test

docs:
	mkdir -p scratch/tmp/site
	uv run mkdocs build --clean -d scratch/tmp/site

lint:
	uv run ruff check --fix .

reformat:
	uv run ruff format .

serve:
	uv run mkdocs serve

setup:
	uv sync --all-groups
	uv run pre-commit install --install-hooks

test:
	uv run pytest -x --cov

type_check:
	uv run mypy tests --ignore-missing-import

################################################################################

.PHONY: \
	build \
	docs \
	help \
	lint \
	reformat \
	setup \
	test \
	type_check
