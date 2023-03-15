from dagster import Definitions, load_assets_from_package_module

from jaffle import assets
from jaffle.duckpond import DuckPondIOManager

DUCKDB_LOCAL_CONFIG = """
set s3_access_key_id='test';
set s3_secret_access_key='test';
set s3_endpoint='localhost:4566';
set s3_use_ssl='false';
set s3_url_style='path';
"""

defs = Definitions(
    assets=load_assets_from_package_module(assets),
    resources={
        "io_manager": DuckPondIOManager(bucket_name="datalake", duckdb_options=DUCKDB_LOCAL_CONFIG)
    },
)
