.PHONY: init coverage release test update

RELEASE_TYPE=patch

init:
	pip install pipenv
	pipenv lock
	pipenv install --dev

coverage:
	pipenv run py.test --cov=pit --cov-report term-missing tests

release:
	pipenv run bumpversion $(RELEASE_TYPE)
	docker build -t gcr.io/mitlib-adit/pit:$(shell git describe --tag) .

test:
	pipenv run py.test tests --tb=short

update:
	pipenv update --dev
