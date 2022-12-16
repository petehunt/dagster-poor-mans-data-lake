from dagster import Definitions, load_assets_from_package_module, io_manager

from jaffle import assets

from jaffle.duckpond import DuckPondIOManager, DuckDB


duckdb_res = DuckDB("""
set s3_access_key_id='test';
set s3_secret_access_key='test';
set s3_endpoint='localhost:4566';
set s3_use_ssl='false';
set s3_url_style='path';
"""
)


@io_manager(required_resource_keys={"duckdb"})
def duckpond_io_manager(init_context):
    return DuckPondIOManager("datalake", init_context.resources.duckdb)


defs =  Definitions(
    assets=load_assets_from_package_module(assets),
    resources={"io_manager": duckpond_io_manager, "duckdb": duckdb_res},
)