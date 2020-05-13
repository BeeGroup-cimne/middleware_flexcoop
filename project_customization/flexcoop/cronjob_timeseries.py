from datetime import datetime, timedelta
import sys

from pymongo import UpdateOne, ReplaceOne, DeleteMany


sys.path.extend([sys.argv[1]])
from mongo_orm import MongoDB, AnyField
from project_customization.flexcoop.models import DataPoint, Device
from project_customization.flexcoop.reports.telemetry_usage import get_data_model
from project_customization.flexcoop.timeseries_utils import timeseries_mapping, indoor_sensing, occupancy, meter, \
    status_devices, device_status
from project_customization.flexcoop.utils import convert_snake_case

import pandas as pd
import numpy as np
import pytz
"""We define the cronjobs to be executed to deal with the raw data recieved"""

#define the final timeseries models:
timezone = pytz.timezone("Europe/Madrid")

def no_outliers_stats(series, lowq=2.5, highq=97.5):
  hh = series[(series <= np.nanquantile(series, highq/100))& (series >= np.nanquantile(series, lowq/100))]
  return {"mean": hh.mean(), "median": hh.median(), "std": hh.std()}

def clean_znorm_data(series, th, lowq=2.5, highq=97.5):
    stats = no_outliers_stats(series, lowq, highq)
    zscore = np.abs( (series - stats['median']) / stats['std'])
    return series[zscore < th]

def clean_threshold_data(series, min_th=None, max_th=None):
    if min_th and max_th:
        return series[(min_th<=series) & (series<=max_th)]
    elif min_th:
        return series[series >= min_th]
    elif max_th:
        return series[series <= max_th]
    else:
        return series

def aggregate_device_status(now):
    today = timezone.localize(datetime(now.year,now.month,now.day)).astimezone(pytz.UTC)
    devices = set()
    for key, value in status_devices.items():
        raw_model = get_data_model(key)
        devices.update(raw_model.__mongo__.distinct("device_id"))
    # iterate for each device to obtain the clean data of each type.
    for device in devices:
        point = Device.find_one({"device_id": device})
        if not point:
            continue
        device_df = []
        #fdsfa
        for key in point.status.keys():
            try:
                database = "{}_{}".format("status",convert_snake_case(key))
                value = status_devices[database]
            except:
                continue

            raw_model = get_data_model(database)
            data = MongoDB.to_dict(raw_model.find({"device_id": device}))
            if not data:
                continue
            df = pd.DataFrame.from_records(data)
            df.index = pd.to_datetime(df.dtstart)
            df = df.sort_index()
            account_id = df.account_id.unique()[0]
            aggregator_id = df.aggregator_id.unique()[0]
            device_class = point.rid
            print("readed data")
            # instant values, expand the value tu the current time
            df = df[['value']].append(pd.DataFrame({"value": np.nan}, index=[now]))
            data_clean = df.fillna(method="pad")
            if data_clean.empty:
                continue
            df = pd.DataFrame(data_clean)
            df = df.rename(columns={"value": value['field']})
            device_df.append(df)
        print("treated data")
        #fdsafdf
        if device_df:
            device_df_final = device_df.pop(0)
            device_df_final = device_df_final.join(device_df, how="outer")
            device_df_final = device_df_final.fillna(method="pad")
            device_df_final['account_id'] = account_id
            device_df_final['aggregator_id'] = aggregator_id
            device_df_final['device_class'] = device_class
            device_df_final['device_id'] = device
            device_df_final['timestamp'] = device_df_final.index.to_pydatetime()
            device_df_final['_created_at'] = datetime.utcnow()
            device_df_final['_updated_at'] = datetime.utcnow()
            device_df_final = device_df_final[device_df_final.index >= today.replace(tzinfo=None)]

            df_ini = min(device_df_final.index)
            df_max = max(device_df_final.index)
            documents = device_df_final.to_dict('records')
            print("writting_sensing_data {}".format(len(documents)))
            device_status.__mongo__.delete_many({"device_id": device, "timestamp": {"$gte":df_ini.to_pydatetime(), "$lte": df_max.to_pydatetime()}})
            device_status.__mongo__.insert_many(documents)


def aggregate_timeseries(freq, now):
    #search for all reporting devices
    today = timezone.localize(datetime(now.year,now.month,now.day)).astimezone(pytz.UTC)
    devices = set()
    for key, value in timeseries_mapping.items():
        raw_model = get_data_model(key)
        devices.update(raw_model.__mongo__.distinct("device_id"))
    #iterate for each device to obtain the clean data of each type.
    for device in devices:
        point = DataPoint.find_one({"device_id": device})
        if not point:
            continue

        indoor_sensing_df = []
        occupancy_df = []
        meter_df = []

        for key in point.reporting_items.keys():
            try:
                value = timeseries_mapping[key]
            except:
                continue
            raw_model = get_data_model(key)
            data = MongoDB.to_dict(raw_model.find({"device_id": device}))
            if not data:
                continue
            df = pd.DataFrame.from_records(data)
            # get the data_point information
            point_info = point.reporting_items[key]
            reading_type = point_info['reading_type']
            df.index = pd.to_datetime(df.dtstart)
            df = df.sort_index()
            account_id = df.account_id.unique()[0]
            aggregator_id = df.aggregator_id.unique()[0]
            device_class = point.rid

            print("readed data")
            if reading_type == "Direct Read":
                if value['operation'] == "SUM":
                    try:
                        df.value = pd.to_numeric(df.value)
                    except:
                        print("AVG is only valid for numeric values")
                        continue
                    data_clean = df.resample("1s").mean().interpolate().resample(freq).mean().diff().dropna()
                    if value['cleaning']:
                        for method, parameters in value['cleaning'].items():
                            if method == "znorm":
                                data_clean.value = clean_znorm_data(data_clean.value, parameters)
                            elif method == "threshold":
                                data_clean.value = clean_threshold_data(data_clean.value, min_th=parameters[0], max_th=parameters[1])


                else:
                    data_clean = pd.DataFrame()

            elif reading_type == "Net":
                # instant values, expand the value tu the current time
                df = df[['value']].append(pd.DataFrame({"value": np.nan}, index=[now]))
                if value['operation'] == "AVG":
                    # average is applied to numeric values
                    try:
                        df.value = pd.to_numeric(df.value)
                    except:
                        print("AVG is only valid for numeric values")
                        continue
                    data_clean = df.resample("1s").pad().dropna().resample(freq).mean()
                    if value['cleaning']:
                        for method, parameters in value['cleaning'].items():
                            if method == "znorm":
                                data_clean.value = clean_znorm_data(data_clean.value, parameters)
                            elif method == "threshold":
                                data_clean.value = clean_threshold_data(data_clean.value, min_th=parameters[0], max_th=parameters[1])

                elif value['operation'] == "FIRST":
                    # first is applied to all types
                    data_clean = df.resample("1s").pad().dropna().resample(freq).first()

                elif value['operation'] == "MAX":
                    # max is applied to numeric values
                    try:
                        df.value = pd.to_numeric(df.value)
                    except:
                        print("MAX is only valid for numeric values")
                        continue

                    data_clean = df.resample("1s").pad().dropna().resample(freq).max()
                    if value['cleaning']:
                        for method, parameters in value['cleaning'].items():
                            if method == "znorm":
                                data_clean.value = clean_znorm_data(data_clean.value, parameters)
                            elif method == "threshold":
                                data_clean.value = clean_threshold_data(data_clean.value, min_th=parameters[0], max_th=parameters[1])

                else:
                    data_clean = pd.DataFrame()

            else:
                data_clean = pd.DataFrame()


            if data_clean.empty:
                continue

            df = pd.DataFrame(data_clean)
            df = df.rename(columns={"value": value['field']})

            if value['class'] == indoor_sensing:
                indoor_sensing_df.append(df)
            elif value['class'] == occupancy:
                occupancy_df.append(df)
            elif value['class'] == meter:
                meter_df.append(df)
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
            indoor_sensing_final = indoor_sensing_final[indoor_sensing_final.index >= today.replace(tzinfo=None)]
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
            occupancy_final = occupancy_final[occupancy_final.index >= today.replace(tzinfo=None)]

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
            meter_final = meter_final[meter_final.index >= today.replace(tzinfo=None)]
            df_ini = min(meter_final.index)
            df_max = max(meter_final.index)
            documents = meter_final.to_dict('records')
            print("writting_meter_data {}".format(len(documents)))
            meter.__mongo__.delete_many(
                {"device_id": device, "timestamp": {"$gte": df_ini.to_pydatetime(), "$lte": df_max.to_pydatetime()}})
            meter.__mongo__.insert_many(documents)

# Call this function everyday at 00:00, 08:00 and at 16:00
def delete_raw_data():
    now = datetime.now()
    now = timezone.localize(datetime(now.year,now.month,now.day)).astimezone(pytz.UTC)
    # search for all reporting devices
    devices = set()
    for key, value in timeseries_mapping.items():
        raw_model = get_data_model(key)
        devices.update(raw_model.__mongo__.distinct("device_id"))
    # shall to delete all raw data, but leave the last 2 timesteps.
    for device in devices:
        print(device)
        point = DataPoint.find_one({"device_id": device})
        if not point:
            continue
        for key in point.reporting_items.keys():
            try:
                value = timeseries_mapping[key]
            except:
                continue
            raw_model = get_data_model(key)
            data1 = raw_model.__mongo__.find({"device_id": device, "dtstart": {"$lt": now.strftime("%Y-%m-%dT%H:%M:%s.fZ")}}, sort=[("dtstart", -1)])
            delete_date = None
            try:
                if data1[0]:
                    try:
                        delete_date = data1[1]['dtstart']
                    except:
                        delete_date = data1[0]['dtstart']
            except:
                continue

            if delete_date:
                raw_model.__mongo__.delete_many({"device_id": device, "dtstart": {"$lt":delete_date}})

    devices = set()
    for key, value in status_devices.items():
        raw_model = get_data_model(key)
        devices.update(raw_model.__mongo__.distinct("device_id"))

    # shall to delete all raw data, but leave the last 2 timestep.
    for device in devices:
        print(device)
        point = Device.find_one({"device_id": device})
        if not point:
            continue
        for key in point.status.keys():
            try:
                database = "{}_{}".format("status",convert_snake_case(key))
                value = status_devices[database]
            except:
                continue
            raw_model = get_data_model(database)
            data1 = raw_model.__mongo__.find({"device_id": device, "dtstart": {"$lt": now.strftime("%Y-%m-%dT%H:%M:%s.fZ")}}, sort=[("dtstart", -1)])
            delete_date = None
            try:
                if data1[0]:
                    try:
                        delete_date = data1[1]['dtstart']
                    except:
                        delete_date = data1[0]['dtstart']
            except:
                continue

            if delete_date:
                raw_model.__mongo__.delete_many({"device_id": device, "dtstart": {"$lt": delete_date}})

# Call this function every 15 min
def clean_data():
    aggregate_timeseries("15Min", datetime.utcnow())
    aggregate_device_status(datetime.utcnow())

if __name__ == "__main__":
    if sys.argv[2] == "clean":
        clean_data()
    elif sys.argv[2] == "delete":
        delete_raw_data()
    else:
        print("error")
