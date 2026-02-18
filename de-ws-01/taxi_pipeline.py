"""Template for building a `dlt` pipeline to ingest data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def taxi_pipeline_rest_api_source():
    """Define dlt resources from REST API endpoints."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        },
        "resource_defaults": {
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "trips",
                "endpoint": {
                    "path": "",
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        "base_page": 1,
                        "total_path": None,
                        "stop_after_empty_page": True,
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)


if __name__ == "__main__":
    from dlt.destinations import duckdb
    
    pipeline = dlt.pipeline(
        pipeline_name='taxi_pipeline',
        destination=duckdb(credentials="duckdb://taxi_pipeline.duckdb"),
        dataset_name='taxi_data',
        progress="log",
    )
    
    load_info = pipeline.run(taxi_pipeline_rest_api_source())
    print(load_info)  # noqa: T201
