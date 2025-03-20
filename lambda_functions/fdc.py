import numpy as np
import zarr
import json


def lambda_handler(event, context):
    try:
        path_parameters = event.get("pathParameters", {})
        river_id = path_parameters.get('river_id', None)
        resolution = path_parameters.get("resolution", None)
        kind = path_parameters.get("kind", None)
        if river_id is None:
            return {
                'statusCode': 400,
                'body': {
                    'message': 'Missing river_id parameter',
                }
            }
        if resolution not in ('hourly', 'daily',):
            return {
                'statusCode': 400,
                'body': {
                    'message': 'Invalid resolution',
                }
            }
        if kind not in ('total', 'monthly',):
            return {
                'statusCode': 400,
                'body': {
                    'message': 'Invalid FDC type requested: choose total or monthly',
                }
            }

        store = zarr.open_group(f's3://rfs-v2/retrospective/fdc.zarr', storage_options={'anon': True})
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
        try:
            p_exceed = store['p_exceed'][:].tolist()
        except Exception as e:
            return {
                'statusCode': 500,
                'body': 'Unexpected error parsing time labels',
            }

        response = {
            'p_exceed': p_exceed,
        }

        if kind == 'monthly':
            var_name = f'fdc_{resolution}_monthly'
            data = store[var_name][:, :, river_index].round(2)
            for i in range(data.shape[0]):
                response[f'fdc_{i + 1:02}'] = data[i, :].tolist()
        else:
            var_name = f'fdc_{resolution}'
            response['fdc'] = store[var_name][:, river_index].round(2).tolist()

        return {
            'statusCode': 200,
            'body': json.dumps(response),
            "headers": {
                "Content-Type": "application/json"
            }
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }
