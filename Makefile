.PHONY: init coverage release test update
SHELL=/bin/bash

RELEASE_TYPE=patch

init:
	pip install pipenv
	pipenv lock
	pipenv install --dev

coverage:
	pipenv run py.test --cov=pit --cov-report term-missing tests

release:
	pipenv run bumpversion $(RELEASE_TYPE)
	docker build -t gcr.io/mitlib-adit/pit:`git describe --tag` .
	@tput setaf 2
	@echo Built release for `git describe --tag`. Make sure to run:
	@echo "  $$ git push origin <branch> tag `git describe --tag`"
	@echo "  $$ gcloud docker -- push gcr.io/mitlib-adit/pit:`git describe --tag`"
	@tput sgr0

test:
	pipenv run py.test tests --tb=short

update:
	pipenv update --dev
