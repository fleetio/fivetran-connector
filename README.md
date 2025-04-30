# Fleetio Fivetran Connector

This is the code for the Fleetio connector based on the [Fivetran connector SDK](https://fivetran.com/docs/connector-sdk). 

## Fleetio Overview
View the Fleetio website [here](https://fleetio.com). For more information on the Fleetio API, check out [Fleetio API Portal](https://developer.fleetio.com).

## Local development
Before starting, you'll need to install the Fivetran Connector SDK package with `pip install fivetran-connector-sdk`.
Run either `fivetran debug` or `python connector.py` to test the connector. 
To deploy your changes to Fivetran, update the `configuration.json` file with your Fleetio API credentials:
```
{
    "Account-Token": "<accountToken>",
    "Authorization": "Token <ApiKey>"
}
```
and run `fivetran deploy --api-key <FIVETRAN-BASE-64-ENCODED-API-KEY> --destination <DESTINATION-NAME> --connection <CONNECTION-NAME> --configuration configuration.json`.

For more details, read the [SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide).
