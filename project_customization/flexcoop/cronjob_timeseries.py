from datetime import datetime, timedelta
import sys
sys.path.extend([sys.argv[1]])
from mongo_orm import MongoDB, AnyField
from project_customization.flexcoop.models import DataPoint
from project_customization.flexcoop.reports.telemetry_usage import get_data_model
import pandas as pd
"""We define the cronjobs to be executed to deal with the raw data recieved"""

#define the final timeseries models:

class timeseries(MongoDB):
    __collectionname__ = "invalid"
    account_id = AnyField()
    aggregator_id = AnyField()
    device_id = AnyField()
    timestamp = AnyField()
    device_class =  AnyField()

    def __init__(self, account_id, aggregator_id, device_id, device_class, timestamp, **kwargs):
        point = self.find_one({indoor_sensing.device_id(): device_id, indoor_sensing.timestamp(): timestamp})
        if point:
            self._id = point._id
        else:
            self.account_id = account_id
            self.aggregator_id = aggregator_id
            self.device_id = device_id
            self.timestamp = timestamp
            self.device_class = device_class
        for k, v in kwargs.items():
            self.__setattr__(k, v)

class indoor_sensing(timeseries):
    __collectionname__ = "indoor_sensing"
    account_id = AnyField()
    aggregator_id = AnyField()
    device_id = AnyField()
    timestamp = AnyField()
    temperature = AnyField()
    lux = AnyField()
    relhumidity = AnyField()
    airquality = AnyField()
    tvoc = AnyField()
    device_class = AnyField()

    def __init__(self, account_id, aggregator_id, device_id, device_class, timestamp, **kwargs):
        super(indoor_sensing, self).__init__(account_id, aggregator_id, device_id, device_class, timestamp, **kwargs)


class occupancy(timeseries):
    __collectionname__ = "occupancy"

    account_id = AnyField()
    aggregator_id = AnyField()
    device_id = AnyField()
    timestamp = AnyField()
    occupancy = AnyField()
    device_class = AnyField()

    def __init__(self, account_id, aggregator_id, device_id, device_class, timestamp, **kwargs):
        super(occupancy, self).__init__(account_id, aggregator_id, device_id, device_class, timestamp, **kwargs)

class meter(timeseries):
    __collectionname__ = "meter"

    account_id = AnyField()
    aggregator_id = AnyField()
    device_id = AnyField()
    timestamp = AnyField()
    current = AnyField()
    kwh = AnyField()
    voltage = AnyField()
    watts = AnyField()
    device_class = AnyField()

    def __init__(self, account_id, aggregator_id, device_id, device_class, timestamp, **kwargs):
        super(meter, self).__init__(account_id, aggregator_id, device_id, device_class, timestamp, **kwargs)


timeseries_mapping = {
    "ambient_temperature": {"class": indoor_sensing, "field": "temperature", "operation": "AVG"},
    "sensor_temperature": {"class": indoor_sensing, "field": "temperature" ,"operation": "AVG"},
    "temperature": {"class": indoor_sensing, "field": "temperature", "operation": "AVG"},
    "lux": {"class": indoor_sensing, "field": "lux", "operation": "AVG"},
    "sensor_luminance": {"class": indoor_sensing, "field": "lux", "operation": "AVG"},
    "sensor_relhumidity": {"class": indoor_sensing, "field": "relhumidity", "operation": "AVG"},
    "humidity": { "class": indoor_sensing, "field": "relhumidity", "operation": "AVG"},
    "airquality": { "class": indoor_sensing, "field": "airquality", "operation": "FIRST"},
    "tvoc": { "class": indoor_sensing, "field": "tvoc","operation": "MAX"},

    "alarm_motion": { "class": occupancy, "field": "occupancy", "operation": "AVG"},
    "motion": { "class": occupancy, "field": "occupancy", "operation": "AVG"},

    "meter_current": { "class": meter, "field": "current", "operation": "AVG"},
    "meter_kwh": { "class": meter, "field": "kwh", "operation": "SUM"},
    "meter_voltage": { "class": meter, "field": "voltage", "operation": "AVG"},
    "meter_watts": { "class": meter, "field": "watts", "operation": "AVG"},
}

def aggregate_timeseries(freq):
    for key, value in timeseries_mapping.items():
        raw_model = get_data_model(key)
        data = MongoDB.to_dict(raw_model.find({}))
        if not data:
            continue
        df = pd.DataFrame.from_records(data)
        print(key)
        for device, data_device in df.groupby("device_id"):
            # get the data_point information
            point = DataPoint.find_one({"device_id": device})
            point_info = point.reporting_items[key]
            reading_type = point_info['reading_type']
            data_device.index = pd.to_datetime(data_device.dtstart)
            account_id = data_device.account_id.unique()[0]
            aggregator_id = data_device.aggregator_id.unique()[0]
            device_class = point.rid
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

            for ts, v in data_clean.iterrows():
                params = {value['field']: v.value}
                point = value['class'](account_id, aggregator_id, device, device_class, ts, **params)
                point.save()

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
