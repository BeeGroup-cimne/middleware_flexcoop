import uuid

from mongo_orm import MongoDB, AnyField
from datetime import datetime

#Cache of OADR POLL messages. It is done in memory but if too many messages are in the queue it can be moved to the DB
oadrPollQueue = {}

class VEN(MongoDB):
    """
    An openadr VEN
    """
    __collectionname__ = 'virtual_end_node'
    venID = AnyField()
    registrationID = AnyField()
    oadrProfileName = AnyField()
    oadrTransportName = AnyField()
    oadrTransportAddress = AnyField()
    oadrReportOnly = AnyField()
    oadrXmlSignature = AnyField()
    oadrVenName = AnyField()
    oadrHttpPullModel = AnyField()

    def __init__(self, venID, registrationID, oadrProfileName, oadrTransportName, oadrTransportAddress,
                 oadrReportOnly, oadrXmlSignature, oadrVenName, oadrHttpPullModel):
        if venID:
            self.venID = venID
        else:
            self.venID = self.generate_ven_ID()
        if registrationID:
            self.registrationID = registrationID
        self.oadrProfileName = oadrProfileName
        self.oadrTransportName = oadrTransportName
        self.oadrTransportAddress = oadrTransportAddress
        self.oadrReportOnly = oadrReportOnly
        self.oadrXmlSignature = oadrXmlSignature
        self.oadrVenName = oadrVenName
        self.oadrHttpPullModel = oadrHttpPullModel

    def generate_ven_ID(self):
        return str(uuid.uuid1())

    def remove_reports(self):
        ven_reports = MetadataReports.find({MetadataReports.ven(): self._id})
        for report in ven_reports:
            points = DataPoint.find({DataPoint.report(): report._id})
            devices = Device.find({Device.report():reoirt._id})
            for p in points:
                p.delete()
            for d in devices:
                d.delete()
            report.delete()

    def __repr__(self):
        return '<VEN {}>'.format(self.oadrVenName)


class MetadataReports(MongoDB):
    """
       An openadr metadata_report.
    """
    __collectionname__ = 'metadata_reports'
    ven = AnyField()
    eiReportID = AnyField()
    specifierID = AnyField()
    duration = AnyField()
    reportName = AnyField()
    createdDateTime = AnyField()
    subscribed = AnyField()
    def __init__(self, ven, eiReportID, specifierID, duration, reportName, createdDateTime=datetime.utcnow(), subscribed=False):
        self.ven = ven
        self.eiReportID = eiReportID
        self.specifierID = specifierID
        self.duration = duration
        self.reportName = reportName
        self.createdDateTime = createdDateTime
        self.subscribed = subscribed


class DataPoint(MongoDB):
    """
       An openadr Metadata_report data_point
    """
    __collectionname__ = 'data_points'
    report = AnyField()
    rID = AnyField()
    reportSubject = AnyField()
    reportDataSource = AnyField()
    reportType = AnyField()
    reportItem = AnyField()
    readingType = AnyField()
    marketContext = AnyField()
    oadrMinPeriod = AnyField()
    oadrMaxPeriod = AnyField()
    oadrOnChange = AnyField()
    subscribed = AnyField()

    def __init__(self, report, rID, reportSubject, reportDataSource, reportType, reportItem, readingType, marketContext, oadrMinPeriod, oadrMaxPeriod, oadrOnChange, subscribed=False):
        self.report = report
        self.rID = rID
        self.reportSubject = reportSubject
        self.reportDataSource = reportDataSource
        self.reportType = reportType
        self.reportItem = reportItem
        self.readingType = readingType
        self.marketContext = marketContext
        self.oadrMinPeriod = oadrMinPeriod
        self.oadrMaxPeriod = oadrMaxPeriod
        self.oadrOnChange = oadrOnChange
        self.subscribed = subscribed

class Device(MongoDB):
    """
       An openadr Metadata_report device
    """
    __collectionname__ = 'devices'
    report = AnyField()
    rID = AnyField()
    reportSubject = AnyField()
    reportDataSource = AnyField()
    reportType = AnyField()
    reportItem = AnyField()
    readingType = AnyField()
    marketContext = AnyField()
    oadrMinPeriod = AnyField()
    oadrMaxPeriod = AnyField()
    oadrOnChange = AnyField()
    subscribed = AnyField()

    def __init__(self, report, rID, reportSubject, reportDataSource, reportType, reportItem, readingType, marketContext, oadrMinPeriod, oadrMaxPeriod, oadrOnChange, subscribed=False):
        self.report = report
        self.rID = rID
        self.reportSubject = reportSubject
        self.reportDataSource = reportDataSource
        self.reportType = reportType
        self.reportItem = reportItem
        self.readingType = readingType
        self.marketContext = marketContext
        self.oadrMinPeriod = oadrMinPeriod
        self.oadrMaxPeriod = oadrMaxPeriod
        self.oadrOnChange = oadrOnChange
        self.subscribed = subscribed


class ReportsToSend(MongoDB):
    """
    The reports that this VTN has to send to other VEN (pending_reports)
    """
    report = AnyField()
    reportRequestID = AnyField()
    granularity = AnyField()
    reportBackDuration = AnyField()
    relatedDataPoints = AnyField()
    canceled = AnyField()

    __collectionname__ = 'reports_to_send'

    def __init__(self, report, reportRequestID, granularity, reportBackDuration, relatedDataPoints, canceled=False):
        self.report = report
        self.reportRequestID = reportRequestID
        self.granularity = granularity
        self.reportBackDuration = reportBackDuration
        self.relatedDataPoints = relatedDataPoints
        self.canceled = canceled

class Event(MongoDB):
    eventID = AnyField()
    modificationNumber = AnyField()
    modificationDateTime = AnyField()
    modificationReason = AnyField()
    priority = AnyField()
    marketContext = AnyField()
    createdDateTime = AnyField()
    eventStatus = AnyField()
    testEvent = AnyField()
    vtnComment = AnyField()
    components = AnyField()
    dtstart = AnyField()
    duration = AnyField()
    tolerance = AnyField()
    eiNotification = AnyField()
    eiRampUp = AnyField()
    eiRecovery = AnyField()
    __collectionname__ = "dr_events"

    def __init__(self, eventID, priority, marketContext, eventStatus, testEvent, vtnComment, components, dtstart,
                 duration, tolerance, eiNotification, eiRampUp, eiRecovery, responseRequired, createdDateTime=datetime.utcnow()):
        self.eventID = eventID
        self.modificationNumber = str(0)
        self.priority = priority
        self.marketContext = marketContext
        self.createdDateTime = createdDateTime
        self.eventStatus = eventStatus
        self.testEvent = testEvent
        self.vtnComment = vtnComment
        self.components = components
        self.dtstart = dtstart
        self.duration = duration
        self.tolerance = tolerance
        self.eiNotification = eiNotification
        self.eiRampUp = eiRampUp
        self.eiRecovery = eiRecovery
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
                        self.modificationNumber += 1
                        self.modificationDateTime = datetime.utcnow().isoformat()
                        inc = True
                elif key == 'eventStatus':
                    if self.__dict__[key] != self.__dict__[_key] and self.__dict__[key] == "cancelled":
                        self.modificationNumber += 1
                        self.modificationDateTime = datetime.utcnow().isoformat()
                        inc = True
            self.__dict__.__delitem__(_key)
        self._modification_fields = []
        super(Event, self).save()


class EventSignal(MongoDB):
    __collectionname__ = "dr_event_signals"

    event = AnyField()
    signalID = AnyField()
    signalType = AnyField()
    signalName = AnyField()
    itemBase = AnyField()
    currentValue = AnyField()

    def __init__(self, event, signalID, signalType, signalName, itemBase, currentValue):
        self.event = event
        self.signalID = signalID
        self.signalType = signalType
        self.signalName = signalName
        self.itemBase = itemBase
        self.currentValue = currentValue
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
    signalPayload = AnyField()

    def __init__(self, signal, uid, dtstart, duration, signalPayload):
        self.signal = signal
        self.uid = uid
        self.dtstart = dtstart
        self.duration = duration
        self.signalPayload = signalPayload
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


