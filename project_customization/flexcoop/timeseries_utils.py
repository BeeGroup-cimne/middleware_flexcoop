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

class atw_heatpumps(timeseries):
    __collectionname__ = "airtowater"

    def __init__(self):
        super(atw_heatpumps, self).__init__()

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
    "ambient_temperature": {"class": indoor_sensing, "field": "temperature", "operation": "AVG", "cleaning": [{"type": "threshold", "params": [-20, 50]}, {"type":"znorm", "params": 4}]},
    "sensor_temperature": {"class": indoor_sensing, "field": "temperature" ,"operation": "AVG", "cleaning":  [{"type": "threshold", "params": [-20, 50]}, {"type":"znorm", "params": 4}]},
    "temperature": {"class": indoor_sensing, "field": "temperature", "operation": "AVG", "cleaning": [{"type": "threshold", "params": [-20, 50]}, {"type":"znorm", "params": 4}]},
    "lux": {"class": indoor_sensing, "field": "lux", "operation": "AVG", "cleaning": [{"type": "threshold", "params": [0, None]}, {"type":"znorm", "params": 4}]},
    "sensor_luminance": {"class": indoor_sensing, "field": "lux", "operation": "AVG", "cleaning": [{"type": "threshold", "params": [0, None]}, {"type":"znorm", "params": 4}]},
    "sensor_relhumidity": {"class": indoor_sensing, "field": "relhumidity", "operation": "AVG", "cleaning": [{"type": "threshold", "params": [0, None]}, {"type":"znorm", "params": 4}]},
    "humidity": {"class": indoor_sensing, "field": "relhumidity", "operation": "AVG", "cleaning": [{"type": "threshold", "params": [0, None]}, {"type":"znorm", "params": 4}]},
    "airquality": {"class": indoor_sensing, "field": "airquality", "operation": "FIRST", "cleaning": False},
    "tvoc": {"class": indoor_sensing, "field": "tvoc", "operation": "MAX", "cleaning": False},

    "alarm_motion": {"class": occupancy, "field": "occupancy", "operation": "AVG", "cleaning": False},
    "motion": {"class": occupancy, "field": "occupancy", "operation": "AVG", "cleaning": False},

    "meter_current": {"class": meter, "field": "current", "operation": "AVG",
                      "cleaning": [{"type": "threshold", "params": [0, None]}, {"type":"znorm", "params": 4}]},
    "meter_kwh": {"class": meter, "field": "kwh", "operation": "SUM",
                  "cleaning": [{"type": "threshold", "params": [0, None]}, {"type":"znorm", "params": 4}]},
    "meter_voltage": {"class": meter, "field": "voltage", "operation": "AVG",
                      "cleaning": [{"type": "threshold", "params": [0, None]}, {"type":"znorm", "params": 4}]},
    "meter_watts": {"class": meter, "field": "watts", "operation": "AVG",
                    "cleaning": [{"type": "threshold", "params": [0, None]}]},

    "calculatedflowtemp": {"class": atw_heatpumps, "field": "calculatedflowtempC", "operation": "AVG",
                      "cleaning": False},
    "heatmediumflow": {"class": atw_heatpumps, "field": "heatmediumflowC", "operation": "AVG",
                      "cleaning": False},
    "roomtemperature": {"class": atw_heatpumps, "field": "roomtemperatureC", "operation": "AVG",
                      "cleaning": False},
    "returntemp": {"class": atw_heatpumps, "field": "returntempC", "operation": "AVG",
                      "cleaning": False},
    "hotwatertop": {"class": atw_heatpumps, "field": "hotwatertopC", "operation": "AVG",
                      "cleaning": False},
    "hotwatercharging": {"class": atw_heatpumps, "field": "hotwaterchargingC", "operation": "AVG",
                      "cleaning": False},
    "outdoortemp": {"class": atw_heatpumps, "field": "outdoortempC", "operation": "AVG",
                      "cleaning": False},
    "externalflowtemp": {"class": atw_heatpumps, "field": "externalflowtempC", "operation": "AVG",
                      "cleaning": False},
}

status_devices = {
    "status_mode" : {"class": device_status, "field": "mode"},
    "status_operation_state" : {"class": device_status, "field": "operation_state"},
    "status_set_point" : {"class": device_status, "field": "set_point"},
    "status_x-color" : {"class": device_status, "field": "color"},
    "status_x-fanspeed" : {"class": device_status, "field": "fanspeed"},
}