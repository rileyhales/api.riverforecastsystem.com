import numpy as np
import zarr
import json
import datetime as dt


def lambda_handler(event, context):
    try:
        path_parameters = event.get("pathParameters", {})
        river_id = path_parameters.get('river_id', None)
        summary = path_parameters.get("summary", None)
        if river_id is None:
            return {
                'statusCode': 400,
                'body': {
                    'message': 'Missing river_id parameter',
                }
            }
        if summary not in ('stats', 'members', 'records'):
            return {
                'statusCode': 400,
                'body': {
                    'message': 'Invalid resolution',
                }
            }

        date = (dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(hours=12)).strftime("%Y%m%d00")
        store = zarr.open_group(f's3://geoglows-v2-forecasts/{date}.zarr', storage_options={'anon': True})
        try:
            river_id = int(river_id)
            river_index = np.where(store['rivid'][:] == river_id)[0][0]
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
            times = store['time'][:]
        except Exception as e:
            return {
                'statusCode': 500,
                'body': 'Unexpected error parsing time labels',
            }

        # discharge
        q = store['Qout'][:-1, :, river_index].round(2)  # Assuming time is the first dimension
        # find columns with all nan values
        nan_cols = np.all(np.isnan(q), axis=0)
        q = q[:, ~nan_cols]
        times = times[~nan_cols].tolist()

        response = {
            'time': times,
            'timeUnit': units,
        }
        if summary == 'stats':
            response['q'] = np.mean(q, axis=0).tolist()
            response['q_uncertainty_upper'] = np.percentile(q, 80, axis=0).tolist()
            response['q_uncertainty_lower'] = np.percentile(q, 20, axis=0).tolist()
        elif summary == 'members':
            for e in range(q.shape[0]):
                response[f'member_{e}'] = q[e, :].tolist()

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
