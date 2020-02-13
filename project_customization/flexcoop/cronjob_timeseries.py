from datetime import datetime, timedelta
import sys

from pymongo import UpdateOne, ReplaceOne

sys.path.extend([sys.argv[1]])
from mongo_orm import MongoDB, AnyField
from project_customization.flexcoop.models import DataPoint
from project_customization.flexcoop.reports.telemetry_usage import get_data_model
from project_customization.flexcoop.timeseries_utils import timeseries_mapping

import pandas as pd
"""We define the cronjobs to be executed to deal with the raw data recieved"""

#define the final timeseries models:


def aggregate_timeseries(freq):
    for key, value in timeseries_mapping.items():
        raw_model = get_data_model(key)
        data = MongoDB.to_dict(raw_model.find({}))
        if not data:
            continue
        df = pd.DataFrame.from_records(data)
        print(key)
        for device, data_device in df.groupby("device_id"):
            print(device)
            # get the data_point information
            point = DataPoint.find_one({"device_id": device})
            if not key in point.reporting_items:
                continue
            point_info = point.reporting_items[key]
            reading_type = point_info['reading_type']
            data_device.index = pd.to_datetime(data_device.dtstart)
            account_id = data_device.account_id.unique()[0]
            aggregator_id = data_device.aggregator_id.unique()[0]
            device_class = point.rid
            print("readed data")
            if value['operation'] == "AVG":
                data_device.value = pd.to_numeric(data_device.value)
                if reading_type == "Direct Read":  # accumulated
                    data_device.value = data_device.value.diff()
                data_clean = data_device[['value']].resample("1s").mean().interpolate().resample(freq).mean()
            elif value['operation'] == "FIRST":
                try:
                    if reading_type == "Direct Read":  # accumulated
                        data_device.value = data_device.value.diff()
                except:
                    pass
                data_clean = data_device[['value']].resample(freq).first()
            elif value['operation'] == "MAX":
                data_device.value = pd.to_numeric(data_device.value)
                if reading_type == "Direct Read":  # accumulated
                    data_device.value = data_device.value.diff()
                data_clean = data_device[['value']].resample(freq).max()
            elif value['operation'] == "SUM":
                data_device.value = pd.to_numeric(data_device.value)
                if reading_type == "Direct Read":  # accumulated
                    data_device.value = data_device.value.diff()
                data_clean = data_device[['value']].cumsum().resample("1s").mean().interpolate().resample(freq).mean().diff()
            else:
                data_clean = pd.DataFrame()

            print("treated data")
            bulk_write = []
            print("writting_data {}".format(data_clean.count().value))
            for ts, v in data_clean.iterrows():
                params = {'account_id': account_id, 'aggregator_id': aggregator_id, "device_id": device, "device_class": device_class, "timestamp": ts, value['field']: v.value}
                bulk_write.append(ReplaceOne({"device_id": device, "timestamp": ts}, params, True))
            raw_model.__mongo__.bulk_write(bulk_write)

# Call this function everyday at 00:00
def delete_raw_data():
    now = datetime.utcnow() - timedelta(minutes=15)
    for key, value in timeseries_mapping.items():
        raw_model = get_data_model(key)
        data = raw_model.find({})
        for d in data:
            ts = datetime.strptime(d.dtstart, "%Y-%m-%dT%H:%M:%S.%f")
            if ts < now:
                d.delete()


# Call this function every 15 min
def clean_data():
    aggregate_timeseries("15Min")

if __name__ == "__main__":
    if sys.argv[2] == "clean":
        clean_data()
    elif sys.argv[2] == "delete":
        delete_raw_data()
    else:
        print("error")
