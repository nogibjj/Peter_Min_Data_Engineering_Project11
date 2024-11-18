"""Handles CLI commands"""

from mylib.etl import extract, transform_and_load
from mylib.query import query


if __name__ == "__main__":
    extract()
    transform_and_load()
    query()
