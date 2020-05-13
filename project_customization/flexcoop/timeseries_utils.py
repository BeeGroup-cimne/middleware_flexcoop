from mongo_orm import MongoDB, AnyField


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

class device_status(timeseries):
    __collectionname__ = "device_status"
    account_id = AnyField()
    aggregator_id = AnyField()
    device_id = AnyField()
    timestamp = AnyField()
    device_class = AnyField()
    operation_state = AnyField()
    set_point = AnyField()
    mode = AnyField()
    color = AnyField()
    fanspeed=AnyField()
    def __init__(self, account_id, aggregator_id, device_id, device_class, timestamp, **kwargs):
        super(device_status, self).__init__(account_id, aggregator_id, device_id, device_class, timestamp, **kwargs)

timeseries_mapping = {
    "ambient_temperature": {"class": indoor_sensing, "field": "temperature", "operation": "AVG", "cleaning": {"znorm":10, "threshold": [-90, 90]}},
    "sensor_temperature": {"class": indoor_sensing, "field": "temperature" ,"operation": "AVG", "cleaning": {"znorm":10, "threshold": [-90, 90]}},
    "temperature": {"class": indoor_sensing, "field": "temperature", "operation": "AVG", "cleaning": {"znorm":10 ,"threshold":[-90, 90]}},
    "lux": {"class": indoor_sensing, "field": "lux", "operation": "AVG", "cleaning": {"znorm":"10", "threshold": [0, None]}},
    "sensor_luminance": {"class": indoor_sensing, "field": "lux", "operation": "AVG", "cleaning": {"znorm":10, "threshold": [0, None]}},
    "sensor_relhumidity": {"class": indoor_sensing, "field": "relhumidity", "operation": "AVG", "cleaning": {"znorm": 10, "threshold": [0, None]}},
    "humidity": {"class": indoor_sensing, "field": "relhumidity", "operation": "AVG", "cleaning": {"znorm": 10, "threshold": [0, None]}},
    "airquality": {"class": indoor_sensing, "field": "airquality", "operation": "FIRST", "cleaning": False},
    "tvoc": {"class": indoor_sensing, "field": "tvoc", "operation": "MAX", "cleaning": False},

    "alarm_motion": {"class": occupancy, "field": "occupancy", "operation": "AVG", "cleaning": False},
    "motion": {"class": occupancy, "field": "occupancy", "operation": "AVG", "cleaning": False},

    "meter_current": {"class": meter, "field": "current", "operation": "AVG",
                      "cleaning": {"znorm": "10", "threshold": [0, None]}},
    "meter_kwh": {"class": meter, "field": "kwh", "operation": "SUM",
                  "cleaning": {"znorm": "10", "threshold": [0, None]}},
    "meter_voltage": {"class": meter, "field": "voltage", "operation": "AVG",
                      "cleaning": {"znorm": "10", "threshold": [0, None]}},
    "meter_watts": {"class": meter, "field": "watts", "operation": "AVG",
                    "cleaning": {"znorm": "10", "threshold": [0, None]}},
}

status_devices = {
    "status_mode" : {"class": device_status, "field": "mode"},
    "status_operation_state" : {"class": device_status, "field": "operation_state"},
    "status_set_point" : {"class": device_status, "field": "set_point"},
    "status_x-color" : {"class": device_status, "field": "color"},
    "status_x-fanspeed" : {"class": device_status, "field": "fanspeed"},
}