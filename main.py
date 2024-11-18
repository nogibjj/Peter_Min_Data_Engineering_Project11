"""Handles CLI commands"""
from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import query


print('Extracting data...')
extract()

print('Transforming and loading data...')
load()

print('Querying data...')
query()


if __name__ == "__main__":
    extract()
    load()
    query()
