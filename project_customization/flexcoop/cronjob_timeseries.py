import multiprocessing
import os
import time
from datetime import datetime, timedelta
import sys
from functools import partial

from pymongo import UpdateOne, ReplaceOne, DeleteMany, MongoClient
sys.path.extend([sys.argv[1]])
import settings

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
NUM_PROCESSES = 10
DEVICES_BY_PROC = 20

def no_outliers_stats(series, lowq=2.5, highq=97.5):
  hh = series[(series <= np.nanquantile(series, highq/100))& (series >= np.nanquantile(series, lowq/100))]
  return {"mean": hh.mean(), "median": hh.median(), "std": hh.std()}

def clean_znorm_data(series, th, lowq=2.5, highq=97.5):
    stats = no_outliers_stats(series, lowq, highq)
    zscore = np.abs( (series - stats['median']) / stats['std'])
    return series[zscore < th]

def znorm_value(series, window, th, lowq=2.5, highq=97.5):
    val_index = int(window / 2)
    if len(series) < val_index:
        return 0
    current = series.iloc[val_index]
    stats = no_outliers_stats(series, lowq, highq)
    if np.isnan(stats['std']):
        zscore = 0
    else:
        zscore = np.abs((current - stats['median']) / stats['std'])
    return zscore

def clean_znorm_window(series, th, lowq=2.5, highq=97.5):
    zscore = series.rolling(window=49, center=True, min_periods=1).apply(znorm_value, raw=False, args=(49, th))
    return series[zscore < th]

def clean_threshold_data(series, min_th=None, max_th=None):
    if min_th is not None and max_th is not None:
        return series[(min_th<=series) & (series<=max_th)]
    elif min_th is not None:
        return series[series >= min_th]
    elif max_th is not None:
        return series[series <= max_th]
    else:
        return series


def cleaning_data(series, period, operations):
    df = pd.DataFrame(series)
    stats = no_outliers_stats(df.value)
    max_th = abs(stats['median']*2 + (100 * stats['std']))
    df.value = clean_threshold_data(df.value, min_th=None, max_th=max_th)
    for operation in operations:
        if operation['type'] == 'threshold':
            df.value = clean_threshold_data(df.value, min_th=operation['params'][0], max_th=operation['params'][1])
        if operation['type'] == "znorm":
            series_1 = df.value[df.value > np.percentile(df.value, 10)]
            series_1 = clean_znorm_data(series_1, operation['params'])
            if period == "backups":
                #print(len(series))
                series_1 = clean_znorm_window(series_1, operation['params'])
            df.value.update(series_1)
    return df.value


def clean_device_data_status(today, now, devices):
    conn = MongoClient(settings.MONGO_URI)
    databasem = conn.get_database("flexcoop")
    devicep = databasem['devices']
    for device in devices:
        print("starting ", device)
        point = devicep.find_one({"device_id": device})
        if not point:
            continue
        device_df = []
        #fdsfa
        for key in point['status'].keys():
            try:
                database = "{}_{}".format("status",convert_snake_case(key))
                value = status_devices[database]
            except:
                continue

            raw_model = databasem[database]
            data = list(raw_model.find({"device_id": device}))
            print("readed data ", key)
            if not data:
                continue
            df = pd.DataFrame.from_records(data)
            df.index = pd.to_datetime(df.dtstart, errors='coerce')
            df = df[~df.index.isna()]
            df = df.sort_index()
            account_id = df.account_id.unique()[0]
            aggregator_id = df.aggregator_id.unique()[0]
            device_class = point['rid']

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
            print("writting_status_data {}".format(len(documents)))
            databasem['device_status'].delete_many({"device_id": device, "timestamp": {"$gte":df_ini.to_pydatetime(), "$lte": df_max.to_pydatetime()}})
            databasem['device_status'].insert_many(documents)


def aggregate_device_status(now):
    print("********* START STATUS CLEAN {} *************", datetime.now())
    today = timezone.localize(datetime(now.year,now.month,now.day)).astimezone(pytz.UTC)
    devices = set()
    for key, value in status_devices.items():
        raw_model = get_data_model(key)
        devices.update(raw_model.__mongo__.distinct("device_id"))
    devices = list(devices)
    # iterate for each device to obtain the clean data of each type.
    a_pool = multiprocessing.Pool(NUM_PROCESSES)
    devices_per_thread = DEVICES_BY_PROC;
    a_pool.map(partial(clean_device_data_status, today, now), [devices[x:x+devices_per_thread] for x in range(0, len(devices), devices_per_thread)])
    print("********* END STATUS CLEAN {} *************", datetime.now())

def clean_device_data_timeseries(today, now, last_period, freq, period, devices):
    conn = MongoClient(settings.MONGO_URI)
    database = conn.get_database("flexcoop")
    datap = database['data_points']
    for device in devices:
        print("starting ", device)
        point = datap.find_one({"device_id": device})
        if not point:
            continue
        indoor_sensing_df = []
        occupancy_df = []
        meter_df = []
        for key in point['reporting_items'].keys():
            try:
                value = timeseries_mapping[key]
            except:
                continue

            raw_model = database[key]
            data = list(raw_model.find({"device_id": device, "dtstart":{"$lte":now.strftime("%Y-%m-%dT%H:%M:%S.%f"), "$gte": last_period.strftime("%Y-%m-%dT%H:%M:%S.%f")}}))
            if not data:
                #no data in the last period, get the last value ever.
                print("nodata")
                data =list(raw_model.find({"device_id": device, "dtstart": {"$lte": now.strftime("%Y-%m-%dT%H:%M:%S.%f")}}))
                if not data:
                    print("nodata2")
                    continue
                else:
                    print("data2")
                    #get the last value of the request
                    df = pd.DataFrame.from_records(data)
                    df.index = pd.to_datetime(df.dtstart, errors='coerce')
                    df = df[~df.index.isna()]
                    df = df.sort_index()
                    df = df.iloc[[-1]]

            else:
                df = pd.DataFrame.from_records(data)
                df.index = pd.to_datetime(df.dtstart, errors='coerce')
                df = df[~df.index.isna()]
                df = df.sort_index()

            # get the data_point information
            point_info = point['reporting_items'][key]
            reading_type = point_info['reading_type']

            account_id = df.account_id.unique()[0]
            aggregator_id = df.aggregator_id.unique()[0]
            device_class = point['rid']
            df = df.loc[~df.index.duplicated(keep='last')]
            print("readed data ", key)

            if reading_type == "Direct Read":
                if value['operation'] == "SUM":
                    try:
                        df.value = pd.to_numeric(df.value)
                    except:
                        print("AVG is only valid for numeric values")
                        continue
                    data_clean = df.resample("1s").mean().interpolate().diff()
                    data_clean = clean_threshold_data(data_clean, 0 , None)
                    data_clean = clean_znorm_data(data_clean, 3)
                    data_clean = data_clean.fillna(0)
                    data_clean = data_clean.resample(freq).sum()
                    if value['cleaning'] and not data_clean.empty:
                        data_clean.value = cleaning_data(data_clean.value, period, value['cleaning'])
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
                    if value['cleaning'] and not data_clean.empty:
                        data_clean.value = cleaning_data(data_clean.value, period, value['cleaning'])

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
                    if value['cleaning'] and not data_clean.empty:
                        data_clean.value = cleaning_data(data_clean.value, period, value['cleaning'])

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
            indoor_sensing_final = indoor_sensing_final[indoor_sensing_final.index >= last_period.replace(tzinfo=None)]
            if not indoor_sensing_final.empty:
                df_ini = min(indoor_sensing_final.index)
                df_max = max(indoor_sensing_final.index)
                documents = indoor_sensing_final.to_dict('records')
                print("writting_sensing_data {}".format(len(documents)))
                database['indoor_sensing'].delete_many({"device_id": device, "timestamp": {"$gte":df_ini.to_pydatetime(), "$lte": df_max.to_pydatetime()}})
                database['indoor_sensing'].insert_many(documents)

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
            occupancy_final = occupancy_final[occupancy_final.index >= last_period.replace(tzinfo=None)]
            if not occupancy_final.empty:
                df_ini = min(occupancy_final.index)
                df_max = max(occupancy_final.index)
                documents = occupancy_final.to_dict('records')
                print("writting_occupancy_data {}".format(len(documents)))
                database['occupancy'].delete_many(
                    {"device_id": device, "timestamp": {"$gte": df_ini.to_pydatetime(), "$lte": df_max.to_pydatetime()}})
                database['occupancy'].insert_many(documents)

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
            meter_final = meter_final[meter_final.index >= last_period.replace(tzinfo=None)]
            if not meter_final.empty:
                df_ini = min(meter_final.index)
                df_max = max(meter_final.index)
                documents = meter_final.to_dict('records')
                print("writting_meter_data {}".format(len(documents)))
                database['meter'].delete_many(
                    {"device_id": device, "timestamp": {"$gte": df_ini.to_pydatetime(), "$lte": df_max.to_pydatetime()}})
                database['meter'].insert_many(documents)

def aggregate_timeseries(freq, now, period):
    #search for all reporting devices
    print("********* START CLEAN {} *************", datetime.now())
    today = timezone.localize(datetime(now.year,now.month,now.day)).astimezone(pytz.UTC)
    devices = set()
    if period == "backups":
        last_period = today - timedelta(days=7)
    else:
        last_period = now - timedelta(hours=12)

    for key, value in timeseries_mapping.items():
        raw_model = get_data_model(key)
        devices.update(raw_model.__mongo__.distinct("device_id"))
    devices = list(devices)
    #iterate for each device to obtain the clean data of each type.
    a_pool = multiprocessing.Pool(NUM_PROCESSES)
    devices_per_thread = DEVICES_BY_PROC;
    a_pool.map(partial(clean_device_data_timeseries, today, now, last_period, freq, period),
               [devices[x:x + devices_per_thread] for x in range(0, len(devices), devices_per_thread)])

    print("********* FINISH CLEAN {} *************", datetime.now())

# Call this function everyday at 00:00, 08:00 and at 16:00
# def delete_raw_data():
#     now = datetime.now()
#     now = timezone.localize(datetime(now.year,now.month,now.day)).astimezone(pytz.UTC)
#     # search for all reporting devices
#     devices = set()
#     for key, value in timeseries_mapping.items():
#         raw_model = get_data_model(key)
#         devices.update(raw_model.__mongo__.distinct("device_id"))
#     # shall to delete all raw data, but leave the last 2 timesteps.
#     for device in devices:
#         print(device)
#         point = DataPoint.find_one({"device_id": device})
#         if not point:
#             continue
#         for key in point.reporting_items.keys():
#             try:
#                 value = timeseries_mapping[key]
#             except:
#                 continue
#             raw_model = get_data_model(key)
#             data1 = raw_model.__mongo__.find({"device_id": device, "dtstart": {"$lt": now.strftime("%Y-%m-%dT%H:%M:%s.fZ")}}, sort=[("dtstart", -1)])
#             delete_date = None
#             try:
#                 if data1[0]:
#                     try:
#                         delete_date = data1[1]['dtstart']
#                     except:
#                         delete_date = data1[0]['dtstart']
#             except:
#                 continue
#
#             if delete_date:
#                 raw_model.__mongo__.delete_many({"device_id": device, "dtstart": {"$lt":delete_date}})
#
#     devices = set()
#     for key, value in status_devices.items():
#         raw_model = get_data_model(key)
#         devices.update(raw_model.__mongo__.distinct("device_id"))
#
#     # shall to delete all raw data, but leave the last 2 timestep.
#     for device in devices:
#         print(device)
#         point = Device.find_one({"device_id": device})
#         if not point:
#             continue
#         for key in point.status.keys():
#             try:
#                 database = "{}_{}".format("status",convert_snake_case(key))
#                 value = status_devices[database]
#             except:
#                 continue
#             raw_model = get_data_model(database)
#             data1 = raw_model.__mongo__.find({"device_id": device, "dtstart": {"$lt": now.strftime("%Y-%m-%dT%H:%M:%s.fZ")}}, sort=[("dtstart", -1)])
#             delete_date = None
#             try:
#                 if data1[0]:
#                     try:
#                         delete_date = data1[1]['dtstart']
#                     except:
#                         delete_date = data1[0]['dtstart']
#             except:
#                 continue
#
#             if delete_date:
#                 raw_model.__mongo__.delete_many({"device_id": device, "dtstart": {"$lt": delete_date}})

# Call this function every 15 min
    """
    aggregate_timeseries("15Min", datetime.utcnow(), "backups")
    aggregate_timeseries("15Min", datetime.utcnow(), "hourly")
    """
def clean_data(period):
    aggregate_timeseries("15Min", datetime.utcnow(), period)
    aggregate_device_status(datetime.utcnow())

if __name__ == "__main__":
    if sys.argv[2] == "clean":
        # pidfile checking
        pidfile = "flexcoop_clean.PID"
        working_directory = os.path.dirname(os.path.abspath(__file__))
        if sys.argv[3] == "backups":
            if os.path.isfile('{}/{}'.format(working_directory, pidfile)):
                with open('{}/{}'.format(working_directory, pidfile), "r") as pid:
                    last_pid = int(pid.read())
                    try:
                        os.kill(last_pid, 9)
                        time.sleep(3)
                    except OSError:
                        pass

        if os.path.isfile('{}/{}'.format(working_directory, pidfile)):
            with open('{}/{}'.format(working_directory, pidfile), "r") as pid:
                last_pid = int(pid.read())
                try:
                    os.kill(last_pid, 0)
                    exit(0)
                except OSError:
                    pass
        with open('{}/{}'.format(working_directory, pidfile), "w") as pid:
            pid.write(str(os.getpid()))
        clean_data(sys.argv[3])
    elif sys.argv[2] == "delete":
        #delete_raw_data()
    else:
        print("error")
