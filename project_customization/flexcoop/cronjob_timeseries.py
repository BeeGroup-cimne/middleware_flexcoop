import multiprocessing
import os
import time
from datetime import datetime, timedelta
import sys
from functools import partial
import mongo_proxy
from pymongo import UpdateOne, ReplaceOne, DeleteMany, MongoClient
sys.path.extend([sys.argv[1]])
import settings

from mongo_orm import MongoDB, AnyField
from project_customization.flexcoop.models import DataPoint, Device
from project_customization.flexcoop.reports.telemetry_usage import get_data_model
from project_customization.flexcoop.timeseries_utils import timeseries_mapping, indoor_sensing, occupancy, meter, \
    status_devices, device_status, atw_heatpumps
from project_customization.flexcoop.utils import convert_snake_case

import pandas as pd
import numpy as np
import pytz
"""We define the cronjobs to be executed to deal with the raw data recieved"""

#define the final timeseries models:
timezone = pytz.timezone("Europe/Madrid")
NUM_PROCESSES = 10
DEVICES_BY_PROC = 10

device_exception = ["76f899f2-323b-11ea-92d1-ac1f6b403fbc"]
def no_outliers_stats(series, lowq=2.5, highq=97.5):
  hh = series[(series <= np.nanquantile(series, highq/100))& (series >= np.nanquantile(series, lowq/100))]
  return {"mean": hh.mean(), "median": hh.median(), "std": hh.std()}

def clean_znorm_data(series, th, lowq=2.5, highq=97.5):
    series1 = series.round(2).value_counts()
    series1 = series1 / series1.sum()
    series2 = series.copy()
    for c in series1.iteritems():
        if c[1] > 0.20:
            series2 = series[series.round(2) != c[0]]
        else:
            break
    stats = no_outliers_stats(series2, lowq, highq)
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
    for operation in operations:
        if operation['type'] == 'threshold':
            df.value = clean_threshold_data(df.value, min_th=operation['params'][0], max_th=operation['params'][1])
        if operation['type'] == "znorm":
            df.value = clean_znorm_data(df.value, operation['params'])
            # if period == "backups":
            #     #print(len(series))
            #     df.value = clean_znorm_window(df.value, operation['params'])
    return df.value


def clean_device_data_status(today, now, devices):
    conn = mongo_proxy.MongoProxy(MongoClient(settings.MONGO_URI))
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

"""
data_clean = pd.DataFrame(df.value.resample("1s").mean())  
mask = pd.DataFrame(data_clean.copy())
data_clean = mask.copy()
grp = ((mask.notnull() != mask.shift().notnull()).cumsum())
grp['ones'] = 1
mask['value'] = (grp.groupby('value')['ones'].transform('count') < 3600) | data_clean['value'].notnull()
data_clean.value = data_clean.value.interpolate(limit_direction="backward")[mask.value].diff()
data_clean.value = clean_threshold_data(data_clean.value, 0 , 0.004166)
data_clean_value = data_clean.value.resample(freq).mean()
data_clean_value = data_clean_value * 60 * 15   
data_clean = pd.DataFrame(data_clean_value) 
plt.plot(data_clean.value)   
plt.show()

"""


def clean_device_data_timeseries(today, now, last_period, freq, period, device):
    conn = MongoClient(settings.MONGO_URI)
    database = conn.get_database("flexcoop")
    datap = database['data_points']
    print("starting ", device)
    point = datap.find_one({"device_id": device})
    if not point:
        conn.close()
        return
    atw_heatpumps_df = []
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
                data_check = df.value.diff()

                data_clean = df.value[data_check.shift(-1) >=0]
                data_clean = data_clean[data_check >= 0]

                data_clean = pd.DataFrame(data_clean.resample("1s").mean())
                data_clean['verified'] = data_clean.value.notna()
                data_clean.verified = data_clean.verified[data_clean.value.notna()]

                copy = pd.DataFrame(data_clean.value.resample("3H", label='right').max())
                copy['verified'] = False
                copy.value = copy.value.fillna(method='ffill')

                data_clean = pd.concat([data_clean, copy], sort=True)
                data_clean = data_clean[~data_clean.index.duplicated(keep='last')]
                data_clean = data_clean.sort_index()

                data_clean.value = data_clean.value.interpolate(limit_direction="backward").diff()
                data_clean['verified_0'] = data_clean.verified.fillna(method='ffill')
                data_clean['verified_1'] = data_clean.verified.fillna(method='bfill')
                data_clean['verified'] = data_clean.verified_0 & data_clean.verified_1

                data_clean.value = clean_threshold_data(data_clean.value, 0 , 0.004166)

                data_clean_value = data_clean.value.resample(freq).mean()
                data_clean_value = data_clean_value * 60 * 15
                data_clean_verified = data_clean.verified.resample(freq).apply(all)
                data_clean = pd.DataFrame(data_clean_value)
                data_clean['verified_kwh'] = data_clean_verified

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
        elif value['class'] == atw_heatpumps:
            atw_heatpumps_df.append(df)
        else:
            continue
    print("treated data")
        # join all df and save them to mongo.

    if indoor_sensing_df:
        indoor_sensing_final = indoor_sensing_df.pop(0)
        indoor_sensing_final = indoor_sensing_final.join(indoor_sensing_df, how="outer")
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

    if atw_heatpumps_df:
        atw_heatpumps_final = atw_heatpumps_df.pop(0)
        atw_heatpumps_final = atw_heatpumps_final.join(atw_heatpumps_df, how="outer")
        atw_heatpumps_final['account_id'] = account_id
        atw_heatpumps_final['aggregator_id'] = aggregator_id
        atw_heatpumps_final['device_class'] = device_class
        atw_heatpumps_final['device_id'] = device
        atw_heatpumps_final['timestamp'] = atw_heatpumps_final.index.to_pydatetime()
        atw_heatpumps_final['_created_at'] = datetime.utcnow()
        atw_heatpumps_final['_updated_at'] = datetime.utcnow()
        atw_heatpumps_final = atw_heatpumps_final[atw_heatpumps_final.index >= last_period.replace(tzinfo=None)]
        if not atw_heatpumps_final.empty:
            df_ini = min(atw_heatpumps_final.index)
            df_max = max(atw_heatpumps_final.index)
            documents = atw_heatpumps_final.to_dict('records')
            print("writting_sensing_data {}".format(len(documents)))
            database['atw_heatpumps'].delete_many({"device_id": device, "timestamp": {"$gte":df_ini.to_pydatetime(), "$lte": df_max.to_pydatetime()}})
            database['atw_heatpumps'].insert_many(documents)

    if occupancy_df:
        occupancy_final = occupancy_df.pop(0)
        occupancy_final = occupancy_final.join(occupancy_df, how="outer")
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
        meter_final = meter_final.join(meter_df, how="outer")
        print("meter {}".format(str(len(meter_final))))
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
    conn.close()

def aggregate_exceptions(now, last_period):
    conn = MongoClient(settings.MONGO_URI)
    database = conn.get_database("flexcoop")

    df1 = pd.DataFrame.from_records(database['meter'].find({"device_id":"404de147-6428-4454-8aae-19ac7f8e1bc0", "timestamp": {"$lte": now, "$gte": last_period}}))
    df1 = df1.set_index("timestamp")
    df2 = pd.DataFrame.from_records(database['meter'].find({"device_id":"76f73c6a-323b-11ea-92d1-ac1f6b403fbc", "timestamp": {"$lte": now, "$gte": last_period}}))
    df2 = df2.set_index("timestamp")

    df3 = pd.DataFrame(df1.kwh + df2.kwh)
    df3['verified_kwh'] = df1.verified_kwh & df2.verified_kwh
    df3['watts'] = df1.watts + df2.watts
    df3['account_id'] = "764e95f2-2489-54f2-930e-1e336168c6dc"
    df3['aggregator_id'] = "flexcoop.somenergia.coop"
    df3['device_class'] = "prosumerDeviceMetering"
    df3['device_id'] = "76f899f2-323b-11ea-92d1-ac1f6b403fbc"
    df3['timestamp'] = df3.index.to_pydatetime()
    df3['_created_at'] = datetime.utcnow()
    df3['_updated_at'] = datetime.utcnow()
    database['meter'].delete_many({"device_id":"76f899f2-323b-11ea-92d1-ac1f6b403fbc", "timestamp": {"$lte": now, "$gte": last_period}})
    database['meter'].insert_many(df3.to_dict('records'))
    conn.close()
    # #aqui


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
    devices = [x for x in devices if x not in device_exception]
    #iterate for each device to obtain the clean data of each type.
    a_pool = multiprocessing.Pool(NUM_PROCESSES)
    devices_per_thread = DEVICES_BY_PROC;
    a_pool.map(partial(clean_device_data_timeseries, today, now, last_period, freq, period), devices)
    aggregate_exceptions(now, last_period)

    print("********* FINISH CLEAN {} *************", datetime.now())

def aggregate_timeseries_user(freq, now, user):
        # search for all reporting devices
        print("********* START CLEAN {} *************", datetime.now())
        today = timezone.localize(datetime(now.year, now.month, now.day)).astimezone(pytz.UTC)
        last_period = today - timedelta(days=360)
        raw_model = get_data_model('data_points')
        devices = raw_model.__mongo__.distinct("device_id", {"account_id":user})
        devices = list(devices)
        devices = [x for x in devices if x not in device_exception]
        # iterate for each device to obtain the clean data of each type.
        a_pool = multiprocessing.Pool(NUM_PROCESSES)
        devices_per_thread = DEVICES_BY_PROC
        a_pool.map(partial(clean_device_data_timeseries, today, now, last_period, freq, "backups"), devices)

        print("********* FINISH CLEAN {} *************", datetime.now())


def aggregate_timeseries_device(freq, now, device):
    # search for all reporting devices
    today = timezone.localize(datetime(now.year, now.month, now.day)).astimezone(pytz.UTC)
    last_period = today - timedelta(days=360)
    if device not in device_exception:
        print("********* START CLEAN {} *************", datetime.now())
        clean_device_data_timeseries(today, now, last_period, freq, "backups", device)
        print("********* FINISH CLEAN {} *************", datetime.now())
    else:
        aggregate_exceptions(now, last_period)



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
    elif sys.argv[2] == "user":
        aggregate_timeseries_user("15Min", datetime.utcnow(), sys.argv[3])
    elif sys.argv[2] == "device":
        aggregate_timeseries_device("15Min", datetime.utcnow(), sys.argv[3])
    elif sys.argv[2] == "delete":
        pass
        #delete_raw_data()
    else:
        print("error")
