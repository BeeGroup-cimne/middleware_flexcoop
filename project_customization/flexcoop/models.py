import uuid
from flask import request

from mongo_orm import MongoDB, AnyField
from datetime import datetime

#Cache of OADR POLL messages. It is done in memory but if too many messages are in the queue it can be moved to the DB
from project_customization.flexcoop.utils import generate_UUID, parse_rid, get_id_from_rid

oadrPollQueue = {}

class map_rid_device_id(MongoDB):
    __collectionname__ = "map_id"
    device_id = AnyField()
    rid = AnyField()

    @staticmethod
    def get_or_create_deviceID(rid):
        phisical_device = get_id_from_rid(rid)
        maping = map_rid_device_id.find_one({map_rid_device_id.rid():phisical_device})
        if maping:
            return maping.device_id
        else:
            device_id = generate_UUID()
            maping = map_rid_device_id(phisical_device, device_id)
            maping.save()
            return device_id

    def __init__(self, rid, device_id):
        self.rid = rid
        self.device_id = device_id


class VEN(MongoDB):
    """
    An openadr VEN
    """
    __collectionname__ = 'virtual_end_node'
    ven_id = AnyField()
    registration_id = AnyField()
    oadr_profile_name = AnyField()
    oadr_transport_name = AnyField()
    oadr_transport_address = AnyField()
    oadr_report_only = AnyField()
    oadr_xml_signature = AnyField()
    oadr_ven_name = AnyField()
    oadr_http_pull_model = AnyField()
    account = AnyField()

    def __init__(self, ven_id, registration_id, oadr_profile_name, oadr_transport_name, oadr_transport_address,
                 oadr_report_only, oadr_xml_signature, oadr_ven_name, oadr_http_pull_model):
        if ven_id:
            self.ven_id = ven_id
        else:
            self.ven_id = generate_UUID()
        if registration_id:
            self.registration_id = registration_id
        self.oadr_profile_name = oadr_profile_name
        self.oadr_transport_name = oadr_transport_name
        self.oadr_transport_address = oadr_transport_address
        self.oadr_report_only = oadr_report_only
        self.oadr_xml_signature = oadr_xml_signature
        self.oadr_ven_name = oadr_ven_name
        self.oadr_http_pull_model = oadr_http_pull_model
        self.account = request.cert.get_subject() if hasattr(request, "cert") else None

    def remove_reports(self):
        ven_reports = MetadataReports.find({MetadataReports.ven(): self._id})
        for report in ven_reports:
            points = DataPoint.find({DataPoint.report(): report._id})
            devices = Device.find({Device.report():report._id})
            for p in points:
                p.delete()
            for d in devices:
                d.delete()
            report.delete()

    def __repr__(self):
        return '<VEN {}>'.format(self.oadr_ven_name)


class MetadataReports(MongoDB):
    """
       An openadr metadata_report.
    """
    __collectionname__ = 'metadata_reports'
    ven = AnyField()
    ei_report_id = AnyField()
    specifier_id = AnyField()
    duration = AnyField()
    report_name = AnyField()
    created_date_time = AnyField()
    subscribed = AnyField()
    owned = AnyField()

    def __init__(self, ven, ei_report_id, specifier_id, duration, report_name, created_date_time=datetime.utcnow(), subscribed=False):
        self.ven = ven
        self.ei_report_id = ei_report_id
        self.specifier_id = specifier_id
        self.duration = duration
        self.report_name = report_name
        self.created_date_time = created_date_time
        self.subscribed = subscribed
        self.owned = False


class DataPoint(MongoDB):
    """
       An openadr Metadata_report data_point
    """
    __collectionname__ = 'data_points'

    device_id = AnyField()
    report = AnyField()
    rid = AnyField()
    report_subject = AnyField()
    report_data_source = AnyField()
    account = AnyField()
    spaces = AnyField()
    reporting_items = AnyField()

    def __init__(self, device_id, report, rid, report_subject, report_data_source, account, spaces, reporting_items):

        dev_test = DataPoint.find_one({DataPoint.device_id(): device_id})
        if dev_test:
            self._id = dev_test._id
            self.reporting_items = dev_test.reporting_items
            self.reporting_items.update(reporting_items)
        else:
            self.device_id = device_id
            self.reporting_items = reporting_items
        self.report = report
        self.rid = rid
        self.report_subject = report_subject
        self.report_data_source = report_data_source
        self.account = request.cert.get_subject() if hasattr(request, "cert") else None
        self.spaces = spaces




class Device(MongoDB):
    """
       An openadr Metadata_report device
    """
    __collectionname__ = 'devices'
    # private fields
    rid = AnyField()
    report = AnyField()
    report_data_source = AnyField()


    # public fields
    device_id = AnyField()
    report_subject = AnyField() # Type
    account = AnyField()
    availability = AnyField()
    status = AnyField()
    spaces = AnyField()

    def __init__(self, report, device_id, rid, spaces, report_subject, report_data_source, status_item):


        dev_test = Device.find_one({Device.device_id(): device_id})
        if dev_test:
            self._id = dev_test._id
            self.status = dev_test.status
            self.status.update(status_item)
        else:
            self.device_id = device_id
            self.status = status_item
        self.report = report
        self.rid = rid
        self.account = request.cert.get_subject() if hasattr(request, "cert") else None
        self.availability = ""
        self.spaces = spaces
        self.report_subject = report_subject
        self.report_data_source = report_data_source

class ReportsToSend(MongoDB):
    """
    The reports that this VTN has to send to other VEN (pending_reports)
    """
    report = AnyField()
    report_request_id = AnyField()
    granularity = AnyField()
    report_back_duration = AnyField()
    related_data_points = AnyField()
    canceled = AnyField()

    __collectionname__ = 'reports_to_send'

    def __init__(self, report, report_request_id, granularity, report_back_duration, related_data_points, canceled=False):
        self.report = report
        self.report_request_id = report_request_id
        self.granularity = granularity
        self.report_back_duration = report_back_duration
        self.related_data_points = related_data_points
        self.canceled = canceled

class Event(MongoDB):
    event_id = AnyField()
    modification_number = AnyField()
    modification_date_time = AnyField()
    modification_reason = AnyField()
    priority = AnyField()
    market_context = AnyField()
    created_date_time = AnyField()
    event_status = AnyField()
    test_event = AnyField()
    vtn_comment = AnyField()
    components = AnyField()
    dtstart = AnyField()
    duration = AnyField()
    tolerance = AnyField()
    ei_notification = AnyField()
    ei_ramp_up = AnyField()
    ei_recovery = AnyField()
    __collectionname__ = "dr_events"

    def __init__(self, eventID, priority, marketContext, eventStatus, testEvent, vtnComment, components, dtstart,
                 duration, tolerance, eiNotification, eiRampUp, eiRecovery, responseRequired, createdDateTime=datetime.utcnow()):
        self.event_id = eventID
        self.modification_number = str(0)
        self.priority = priority
        self.market_context = marketContext
        self.created_date_time = createdDateTime
        self.event_status = eventStatus
        self.test_event = testEvent
        self.vtn_comment = vtnComment
        self.components = components
        self.dtstart = dtstart
        self.duration = duration
        self.tolerance = tolerance
        self.ei_notification = eiNotification
        self.ei_ramp_up = eiRampUp
        self.ei_recovery = eiRecovery
        self.responseRequired = responseRequired
        self._modification_fields = []

    def __setattr__(self, key, value):
        if '_id' in self.__dict__:
            _key = "_{}".format(key)
            if not _key in self.__dict__:
                self.__dict__[_key]=self.__dict__[key]
                self._modification_fields.append(key)
        super(Event,self).__setattr__(key, value)

    def save(self):
        inc = False
        for key in self._modification_fields:
            _key = "_{}".format(key)
            if not inc:
                if key not in ['createdDateTime', 'currentValue', 'eventStatus','modificationNumber','modificationDateTime','responseRequired']:
                    if self.__dict__[key] != self.__dict__[_key]:
                        self.modification_number += 1
                        self.modification_date_time = datetime.utcnow().isoformat()
                        inc = True
                elif key == 'eventStatus':
                    if self.__dict__[key] != self.__dict__[_key] and self.__dict__[key] == "cancelled":
                        self.modification_number += 1
                        self.modification_date_time = datetime.utcnow().isoformat()
                        inc = True
            self.__dict__.__delitem__(_key)
        self._modification_fields = []
        super(Event, self).save()


class EventSignal(MongoDB):
    __collectionname__ = "dr_event_signals"

    event = AnyField()
    signal_id = AnyField()
    signal_type = AnyField()
    signal_name = AnyField()
    item_base = AnyField()
    current_value = AnyField()

    def __init__(self, event, signalID, signalType, signalName, itemBase, currentValue):
        self.event = event
        self.signal_id = signalID
        self.signal_type = signalType
        self.signal_name = signalName
        self.item_base = itemBase
        self.current_value = currentValue
        self._modification_fields = []

    def __setattr__(self, key, value):
        if '_id' in self.__dict__:
            _key = "_{}".format(key)
            if not _key in self.__dict__:
                self.__dict__[_key]=self.__dict__[key]
                self._modification_fields.append(key)
        super(EventSignal, self).__setattr__(key, value)

    def save(self):
        inc = False
        for key in self._modification_fields:
            _key = "_{}".format(key)
            if not inc:
                if self.__dict__[key] != self.__dict__[_key]:
                    event = Event.find_one({"_id":self.event})
                    event.modificationNumber += 1
                    event.modificationDateTime = datetime.utcnow().isoformat()
                    event.save()
                    inc = True
            self.__dict__.__delitem__(_key)
        super(EventSignal, self).save()


class EventInterval(MongoDB):
    __collectionname__ = "dr_event_intervals"

    signal = AnyField()
    uid = AnyField()
    dtstart = AnyField()
    duration = AnyField()
    signal_payload = AnyField()

    def __init__(self, signal, uid, dtstart, duration, signal_payload):
        self.signal = signal
        self.uid = uid
        self.dtstart = dtstart
        self.duration = duration
        self.signal_payload = signal_payload
        self._modification_fields = []

    def __setattr__(self, key, value):
        if '_id' in self.__dict__:
            _key = "_{}".format(key)
            if not _key in self.__dict__:
                self.__dict__[_key]=self.__dict__[key]
                self._modification_fields.append(key)
        super(EventInterval, self).__setattr__(key, value)

    def save(self):
        inc = False
        for key in self._modification_fields:
            _key = "_{}".format(key)
            if not inc:
                if self.__dict__[key] != self.__dict__[_key]:
                    event = Event.find_one({"_id":EventSignal.find_one({"_id":self.signal}).event})
                    event.modificationNumber += 1
                    event.modificationDateTime = datetime.utcnow().isoformat()
                    event.save()
                    inc = True
            self.__dict__.__delitem__(_key)
        super(EventInterval, self).save()


