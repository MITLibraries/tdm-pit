.PHONY: init coverage test update


init:
	pipenv lock
	pipenv install --dev

coverage:
	pipenv run py.test --cov=pit --cov-report term-missing tests

test:
	pipenv run py.test tests --tb=short

update:
	pipenv update --dev
