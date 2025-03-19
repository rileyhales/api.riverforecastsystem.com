import numpy as np
import zarr
import json


def lambda_handler(event, context):
    try:
        path_parameters = event.get("pathParameters", {})
        river_id = path_parameters.get('river_id', None)
        resolution = path_parameters.get("resolution", "daily")
        if river_id is None:
            return {
                'statusCode': 400,
                'body': {
                    'message': 'Missing river_id parameter',
                }
            }
        if resolution not in ('hourly', 'daily', 'monthly', 'yearly', 'maximums'):
            return {
                'statusCode': 400,
                'body': {
                    'message': 'Invalid resolution',
                }
            }

        store = zarr.open_group(f's3://rfs-v2/retrospective/{resolution}.zarr', storage_options={'anon': True})
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
            units = store['time'].attrs['units']
            # origin_date = units.split(' since ')[1]
            # times = (np.datetime64(f'{origin_date}T00:00:00') + store['time'][:].astype(int).astype(
            #     'timedelta64[s]')).astype(str).tolist()
            times = store['time'][:].tolist()
        except Exception as e:
            return {
                'statusCode': 500,
                'body': 'Unexpected error parsing time labels',
            }

        # discharge
        q = store['Q'][:, river_index].round(2).tolist()  # Assuming time is the first dimension

        return {
            'statusCode': 200,
            'body': json.dumps({
                'q': q,
                'time': times,
                'timeUnit': units,
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
