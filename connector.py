import json
from typing import List, Dict
from flatten_json import flatten
import requests

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

def update_list_of_dicts(list_of_dicts: List[Dict], keys: List, remove: bool = False):
    if remove:
        updated = [{key: d[key] for key in d if key not in keys} for d in list_of_dicts]
    else:
        updated = [{key: d[key] for key in d if key in keys} for d in list_of_dicts]
    return updated

def base_schema():
    # fivetran doesn't seem to allow separate files, so we'll do this for now
    base_schema = [
        {
            "table": "submitted_inspection_forms",
            "primary_key": [
                "id"
            ],
            "request_info": {
                "path": "/v1/submitted_inspection_forms",
                "params": {
                    "per_page": 100
                }
            }
        },
        {
            "table": "issues",
            "primary_key": [
                "id"
            ],
            "columns": {
                "custom_fields": "JSON",
                "attachment_permissions": "JSON",
                "assigned_contacts": "JSON",
                "labels": "JSON"
            },
            "request_info": {
                "path": "/v2/issues",
                "params": {
                    "per_page": 100
                }
            }
        },
        {
            "table": "service_entries",
            "primary_key": [
                "id"
            ],
            "columns": {
                "custom_fields": "JSON",
                "attachment_permissions": "JSON",
                "labels": "JSON"
            },
            "request_info": {
                "path": "/v2/service_entries",
                "params": {
                    "per_page": 100
                }
            }
        },
        {
            "table": "vehicles",
            "primary_key": [
                "id"
            ],
            "columns": {
                "custom_fields": "JSON",
                "labels": "JSON"
            },
            "request_info": {
                "path": "/v1/vehicles",
                "params": {
                    "per_page": 100
                }
            }
        },
        {
            "table": "expense_entries",
            "primary_key": [
                "id"
            ],
            "request_info": {
                "path": "/v1/expense_entries",
                "params": {
                    "per_page": 100
                }
            }
        },
        {
            "table": "contacts",
            "primary_key": [
                "id"
            ],
            "request_info": {
                "path": "/v2/contacts",
                "params": {
                    "per_page": 100
                }
            }
        },
        {
            "table": "fuel_entries",
            "primary_key": [
                "id"
            ],
            "request_info": {
                "path": "/v1/fuel_entries",
                "params": {
                    "per_page": 100
                }
            }
        },
        {
            "table": "parts",
            "primary_key": [
                "id"
            ],
            "request_info": {
                "path": "/v1/parts",
                "params": {
                    "per_page": 100
                }
            }
        },
        {
            "table": "purchase_orders",
            "primary_key": [
                "id"
            ],
            "columns": {
                "custom_fields": "JSON",
                "labels": "JSON"
            },
            "request_info": {
                "path": "/v1/purchase_orders",
                "params": {
                    "per_page": 100
                }
            }
        },
        {
            "table": "vehicle_assignments",
            "primary_key": [
                "id"
            ],
            "request_info": {
                "path": "/v1/vehicle_assignments",
                "params": {
                    "per_page": 100
                }
            }
        }
    ]
    return base_schema
    
# Define the schema function which lets you configure the schema your connector delivers.
def schema(configuration: dict) -> List[Dict[str, List]]:
    keys_to_include = [
            "table", 
            "primary_key", 
            "columns",
        ]
    base = base_schema()
    schema = update_list_of_dicts(base, keys_to_include)
    
    log.info("Loading schema for tables")

    return schema

def continue_pagination(response_json):
    params = {}
    has_more_pages = True

    next_cursor = response_json.get("next_cursor", None)
    if next_cursor is not None:
        params = {"next_cursor": next_cursor}
    else:
        has_more_pages = False
    return has_more_pages, params

def make_api_request(base_url, path, headers, params=None):
    log.info("Attempting to retrieve data from API")
    try:
        log.info(f"Making API GET request to {path}")
        url = base_url + path
        response = requests.get(url=url, headers=headers, params=params)
        return response.json()
    
    except requests.RequestException as e:
        log.severe(f"API call failed: {str(e)}")
        return None

def update(configuration: dict, state: dict):
    log.info("Starting Fleetio data sync!")

    base_url = "https://secure.fleetio.com/api"
    base = base_schema()
    additional_configuration =  {"Accept": "application/json",
                                "X-Api-Version": "2024-03-15", 
                                "X-Client-Name": "data_connector",
                                "X-Client-Platform": "fleetio_fivetran"
                                }
    headers = {**configuration, **additional_configuration}
    for schema in base:
        log.info(f"Starting sync for {schema['table']}")
        yield from sync_table(
                        base_url=base_url,
                        path=schema["request_info"]["path"],
                        headers=headers,
                        params=schema["request_info"]["params"],
                        table=schema["table"],
                    )

def sync_table(base_url, path, headers, params, table):
    has_more_pages = True
    
    while has_more_pages:
        response = make_api_request(base_url, path, headers, params)
        if not response:
            return
        
        data = response.get("records", [])
        if not data:
            break

        log.info(f"Processing data for {path}")
        for item in data:
            flat_item = flatten(item)
            yield op.upsert(table=table, data=flat_item)
        
        has_more_pages, params = continue_pagination(response)


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    try:
        with open("configuration.json", 'r') as f:
            configuration = json.load(f)
    except FileNotFoundError:
        log.info("Using empty configuration!")
        configuration = {}
    connector.debug(configuration=configuration)