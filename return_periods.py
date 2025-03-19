import numpy as np
import zarr
import json


def lambda_handler(event, context):
    try:
        path_parameters = event.get("pathParameters", {})
        river_id = path_parameters.get('river_id', None)
        distribution = path_parameters.get("distribution", "logpearson3")
        if river_id is None:
            return {
                'statusCode': 400,
                'body': {
                    'message': 'Missing river_id parameter',
                }
            }
        if distribution not in ('logpearson3' 'gumbel'):
            return {
                'statusCode': 400,
                'body': {
                    'message': 'Invalid distribution',
                }
            }

        store = zarr.open_group(f's3://rfs-v2/retrospective/return-periods.zarr', storage_options={'anon': True})
        try:
            river_id = int(river_id)
            river_index = np.where(store['river_id'][:] == river_id)[0][0]
        except Exception as e:
            return {
                'statusCode': 400,
                'body': {
                    'message': 'River ID not found',
                    'hint': f'{type(river_id)}',
                }
            }

        # times
        try:
            returnperiods = store['return_period'][:]
        except Exception as e:
            return {
                'statusCode': 500,
                'body': 'Unexpected error parsing return periods',
            }

        # discharge
        q = store[distribution][:, river_index].round(2).tolist()  # Assuming time is the first dimension

        return {
            'statusCode': 200,
            'body': json.dumps({
                'q': q,
                'return_periods': returnperiods.tolist(),
            }),
            "headers": {
                "Content-Type": "application/json"
            }
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }

print(lambda_handler({'pathParameters': {'river_id': '140266377'}}, None))