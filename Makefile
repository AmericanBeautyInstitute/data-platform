help:
	cat Makefile

################################################################################

ci:
	uv sync --all-groups
	make format
	make lint
	make type-check
	make test

lint:
	uv run ruff check --fix .

format:
	uv run ruff format .

setup:
	uv sync --all-groups
	uv run pre-commit install --install-hooks

test:
	uv run pytest --cov

type-check:
	uv run ty check api config orchestration sources tests

################################################################################

docs:
	uv run --locked --group docs mkdocs build --strict

################################################################################

mutate:
	uv run mutmut run
	uv run mutmut results

################################################################################

ssh:
	gcloud compute ssh dagster-daemon \
		--zone=us-east1-b \
		--tunnel-through-iap \
		--project=american-beauty-institute

deploy:
	gcloud compute ssh dagster-daemon \
		--zone=us-east1-b \
		--tunnel-through-iap \
		--project=american-beauty-institute \
		--command="cd /home/dagster/data-platform && sudo -u dagster git pull && sudo -u dagster /home/dagster/.local/bin/uv sync && sudo systemctl restart dagster-code && sudo systemctl restart dagster"

################################################################################

.PHONY: \
	ci \
	deploy \
	docs \
	format \
	help \
	lint \
	mutate \
	setup \
	ssh \
	test \
	type-check
