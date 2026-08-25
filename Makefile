help:
	cat Makefile

################################################################################

ci:
	uv sync --all-groups
	make reformat
	make lint
	make type_check
	make test

lint:
	uv run ruff check --fix .

reformat:
	uv run ruff format .

setup:
	uv sync --all-groups
	uv run pre-commit install --install-hooks

test:
	uv run pytest -x --cov

type_check:
	uv run ty check tests

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
	build \
	deploy \
	docs \
	help \
	lint \
	mutate \
	reformat \
	setup \
	ssh \
	test \
	type_check
