install:
	pip install --upgrade pip && pip install -r requirements.txt

format:
	black *.py

lint:
	ruff check *.py mylib/*.py

test:
	python -m pytest -vv --cov=main --cov=mylib test_*.py

all: install format lint test

refactor: format lint