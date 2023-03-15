from string import Template
from typing import Mapping

import pandas as pd
from dagster import ConfigurableIOManager
from dagster._utils.cached_method import cached_method
from duckdb import connect
from sqlescapy import sqlescape


class SQL:
    def __init__(self, sql, **bindings):
        self.sql = sql
        self.bindings = bindings


def sql_to_string(s: SQL) -> str:
    replacements = {}
    for key, value in s.bindings.items():
        if isinstance(value, pd.DataFrame):
            replacements[key] = f"df_{id(value)}"
        elif isinstance(value, SQL):
            replacements[key] = f"({sql_to_string(value)})"
        elif isinstance(value, str):
            replacements[key] = f"'{sqlescape(value)}'"
        elif isinstance(value, (int, float, bool)):
            replacements[key] = str(value)
        elif value is None:
            replacements[key] = "null"
        else:
            raise ValueError(f"Invalid type for {key}")
    return Template(s.sql).safe_substitute(replacements)


def collect_dataframes(s: SQL) -> Mapping[str, pd.DataFrame]:
    dataframes = {}
    for key, value in s.bindings.items():
        if isinstance(value, pd.DataFrame):
            dataframes[f"df_{id(value)}"] = value
        elif isinstance(value, SQL):
            dataframes.update(collect_dataframes(value))
    return dataframes


class DuckDB:
    def __init__(self, options=""):
        self.options = options

    def query(self, select_statement: SQL):
        db = connect(":memory:")
        db.query("install httpfs; load httpfs;")
        db.query(self.options)

        dataframes = collect_dataframes(select_statement)
        for key, value in dataframes.items():
            db.register(key, value)

        result = db.query(sql_to_string(select_statement))
        if result is None:
            return
        return result.df()


class DuckPondIOManager(ConfigurableIOManager):
    bucket_name: str
    duckdb_options: str
    prefix: str = ""

    @property
    def duckdb(self) -> DuckDB:
        return DuckDB(self.duckdb_options)

    def _get_s3_url(self, context):
        if context.has_asset_key:
            id = context.get_asset_identifier()
        else:
            id = context.get_identifier()
        return f"s3://{self.bucket_name}/{self.prefix}{'/'.join(id)}.parquet"

    def handle_output(self, context, select_statement: SQL):
        if select_statement is None:
            return

        if not isinstance(select_statement, SQL):
            raise ValueError(f"Expected asset to return a SQL; got {select_statement!r}")

        self.duckdb.query(
            SQL(
                "copy $select_statement to $url (format parquet)",
                select_statement=select_statement,
                url=self._get_s3_url(context),
            )
        )

    def load_input(self, context) -> SQL:
        return SQL("select * from read_parquet($url)", url=self._get_s3_url(context))
