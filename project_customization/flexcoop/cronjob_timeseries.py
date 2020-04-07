from datetime import datetime, timedelta
import sys

from pymongo import UpdateOne, ReplaceOne, DeleteMany

sys.path.extend([sys.argv[1]])
from mongo_orm import MongoDB, AnyField
from project_customization.flexcoop.models import DataPoint, Device
from project_customization.flexcoop.reports.telemetry_usage import get_data_model
from project_customization.flexcoop.timeseries_utils import timeseries_mapping, indoor_sensing, occupancy, meter, \
    status_devices, device_status

import pandas as pd
"""We define the cronjobs to be executed to deal with the raw data recieved"""

#define the final timeseries models:

def aggregate_device_status():
    devices = set()
    device_status_data = []
    for key, value in status_devices.items():
        raw_model = get_data_model(key)
        devices.update(raw_model.__mongo__.distinct("device_id"))
        if value['class'] == device_status:
            device_status_data.append(key)
    for device in devices:
        device_df = []
        data_clean = pd.DataFrame()
        device_mongo = Device.find_one({"device_id": device})
        account_id = device_mongo.account_id
        aggregator_id = device_mongo.aggregator_id
        device_class = device_mongo.rid
        for key, value in status_devices.items():
            print(value['field'])
            raw_model = get_data_model(key)
            data = MongoDB.to_dict(raw_model.find({"device_id": device}))
            if not data:
                continue
            df = pd.DataFrame.from_records(data)
            df.index = pd.to_datetime(df.dtstart)
            df = df.sort_index()
            df = df[['value']].resample('1s').first()
            if not df.empty:
                data_clean[value['field']] = df.value
        print("readed data")
        data_clean = data_clean.dropna(how="all")
        data_clean['account_id'] = account_id
        data_clean['aggregator_id'] = aggregator_id
        data_clean['device_class'] = device_class
        data_clean['device_id'] = device
        data_clean['timestamp'] = data_clean.index.to_pydatetime()
        data_clean['_created_at'] = datetime.utcnow()
        data_clean['_updated_at'] = datetime.utcnow()
        df_ini = min(data_clean.index)
        df_max = max(data_clean.index)
        documents = data_clean.to_dict('records')
        print("writting_sensing_data {}".format(len(documents)))
        device_status.__mongo__.delete_many({"device_id": device, "timestamp": {"$gte":df_ini.to_pydatetime(), "$lte": df_max.to_pydatetime()}})
        device_status.__mongo__.insert_many(documents)

def aggregate_timeseries(freq):
    #search for all reporting devices
    devices = set()
    indoor_sensing_raw_data = []
    occupancy_raw_data = []
    meter_raw_data = []
    for key, value in timeseries_mapping.items():
        raw_model = get_data_model(key)
        devices.update(raw_model.__mongo__.distinct("device_id"))
        if value['class'] == indoor_sensing:
            indoor_sensing_raw_data.append(key)
        elif value['class'] == occupancy:
            occupancy_raw_data.append(key)
        elif value['class'] == meter:
            meter_raw_data.append(key)
    #iterate for each device to obtain the clean data of each type.
    for device in devices:
        indoor_sensing_df = []
        occupancy_df = []
        meter_df = []
        print(device)
        for key, value in timeseries_mapping.items():
            raw_model = get_data_model(key)
            data = MongoDB.to_dict(raw_model.find({"device_id": device}))
            if not data:
                continue
            print(key)
            df = pd.DataFrame.from_records(data)
            # get the data_point information
            point = DataPoint.find_one({"device_id": device})
            if not point or not key in point.reporting_items:
                continue
            point_info = point.reporting_items[key]
            reading_type = point_info['reading_type']
            df.index = pd.to_datetime(df.dtstart)
            df = df.sort_index()
            account_id = df.account_id.unique()[0]
            aggregator_id = df.aggregator_id.unique()[0]
            device_class = point.rid
            print("readed data")
            if value['operation'] == "AVG":
                df.value = pd.to_numeric(df.value)
                if reading_type == "Direct Read":  # accumulated
                    df.value = df.value.diff()
                data_clean = df[['value']].resample("1s").mean().interpolate().resample(freq).mean()
            elif value['operation'] == "FIRST":
                try:
                    if reading_type == "Direct Read":  # accumulated
                        df.value = df.value.diff()
                except:
                    pass
                data_clean = df[['value']].resample(freq).first()
            elif value['operation'] == "MAX":
                df.value = pd.to_numeric(df.value)
                if reading_type == "Direct Read":  # accumulated
                    df.value = df.value.diff()
                data_clean = df[['value']].resample(freq).max()
            elif value['operation'] == "SUM":
                df.value = pd.to_numeric(df.value)
                if reading_type == "Direct Read":  # accumulated
                    df.value = df.value.diff()
                data_clean = df[['value']].cumsum().resample("1s").mean().interpolate().resample(freq).mean().diff()
            else:
                data_clean = pd.DataFrame()

            if data_clean.empty:
                continue
            data_clean[value['field']] = data_clean['value']
            data_clean = data_clean.drop("value", axis=1)
            if key in indoor_sensing_raw_data:
                indoor_sensing_df.append(data_clean)
            elif key in occupancy_raw_data:
                occupancy_df.append(data_clean)
            elif key in meter_raw_data:
                meter_df.append(data_clean)
            else:
                continue
        print("treated data")
        # join all df and save them to mongo.

        if indoor_sensing_df:
            indoor_sensing_final = indoor_sensing_df.pop(0)
            indoor_sensing_final = indoor_sensing_final.join(indoor_sensing_df)
            indoor_sensing_final['account_id'] = account_id
            indoor_sensing_final['aggregator_id'] = aggregator_id
            indoor_sensing_final['device_class'] = device_class
            indoor_sensing_final['device_id'] = device
            indoor_sensing_final['timestamp'] = indoor_sensing_final.index.to_pydatetime()
            indoor_sensing_final['_created_at'] = datetime.utcnow()
            indoor_sensing_final['_updated_at'] = datetime.utcnow()
            df_ini = min(indoor_sensing_final.index)
            df_max = max(indoor_sensing_final.index)
            documents = indoor_sensing_final.to_dict('records')
            print("writting_sensing_data {}".format(len(documents)))
            indoor_sensing.__mongo__.delete_many({"device_id": device, "timestamp": {"$gte":df_ini.to_pydatetime(), "$lte": df_max.to_pydatetime()}})
            indoor_sensing.__mongo__.insert_many(documents)

        if occupancy_df:
            occupancy_final = occupancy_df.pop(0)
            occupancy_final = occupancy_final.join(occupancy_df)
            occupancy_final['account_id'] = account_id
            occupancy_final['aggregator_id'] = aggregator_id
            occupancy_final['device_class'] = device_class
            occupancy_final['device_id'] = device
            occupancy_final['timestamp'] = occupancy_final.index.to_pydatetime()
            occupancy_final['_created_at'] = datetime.utcnow()
            occupancy_final['_updated_at'] = datetime.utcnow()
            df_ini = min(occupancy_final.index)
            df_max = max(occupancy_final.index)
            documents = occupancy_final.to_dict('records')
            print("writting_occupancy_data {}".format(len(documents)))
            occupancy.__mongo__.delete_many(
                {"device_id": device, "timestamp": {"$gte": df_ini.to_pydatetime(), "$lte": df_max.to_pydatetime()}})
            occupancy.__mongo__.insert_many(documents)

        if meter_df:
            meter_final = meter_df.pop(0)
            meter_final = meter_final.join(meter_df)
            meter_final['account_id'] = account_id
            meter_final['aggregator_id'] = aggregator_id
            meter_final['device_class'] = device_class
            meter_final['device_id'] = device
            meter_final['timestamp'] = meter_final.index.to_pydatetime()
            meter_final['_created_at'] = datetime.utcnow()
            meter_final['_updated_at'] = datetime.utcnow()
            df_ini = min(meter_final.index)
            df_max = max(meter_final.index)
            documents = meter_final.to_dict('records')
            print("writting_meter_data {}".format(len(documents)))
            meter.__mongo__.delete_many(
                {"device_id": device, "timestamp": {"$gte": df_ini.to_pydatetime(), "$lte": df_max.to_pydatetime()}})
            meter.__mongo__.insert_many(documents)

# Call this function everyday at 00:00, 08:00 and at 16:00
def delete_raw_data():
    now = datetime.utcnow() - timedelta(minutes=15)
    for key, value in timeseries_mapping.items():
        raw_model = get_data_model(key)
        raw_model.__mongo__.delete_many({"dtstart": {"$lt": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}})

    for key, value in status_devices.items():
        raw_model = get_data_model(key)
        raw_model.__mongo__.delete_many({"dtstart": {"$lt": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}})


# Call this function every 15 min
def clean_data():
    aggregate_timeseries("15Min")
    aggregate_device_status()

if __name__ == "__main__":
    if sys.argv[2] == "clean":
        clean_data()
    elif sys.argv[2] == "delete":
        delete_raw_data()
    else:
        print("error")
