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