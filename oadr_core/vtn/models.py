import uuid

from mongo_orm import MongoDB, AnyField
from datetime import datetime

#Cache of OADR POLL messages. It is done in memory but if too many messages are in the queue it can be moved to the DB
oadrPollQueue = {}

class VEN(MongoDB):
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

    def __repr__(self):
        return '<VEN {}>'.format(self.oadrVenName)


class MetadataReportSpec(MongoDB):
    __collectionname__ = 'metadata_reports'
    ven = AnyField()
    owned = AnyField()
    reportID = AnyField()
    specifierID = AnyField()
    duration = AnyField()
    name = AnyField()
    created = AnyField()
    subscribed = AnyField()

    def __init__(self, ven, owned, reportID, specifierID, duration, name, created=datetime.utcnow(), subscribed=False):
        self.ven = ven
        self.owned = owned
        self.reportID = reportID
        self.specifierID = specifierID
        self.duration = duration
        self.name = name
        self.created = created
        self.subscribed = subscribed


class DataPoint(MongoDB):
    __collectionname__ = 'datapoints'
    rid = AnyField()
    report = AnyField()
    report_subject = AnyField()
    report_source = AnyField()
    report_type = AnyField()
    #report_item = Column(Enum(DataPointItemEnum), nullable=True)
    report_reading = AnyField()
    market_context = AnyField()
    min_sampling = AnyField()
    max_sampling = AnyField()
    onChange = AnyField()

    def __init__(self, rid, report, subject, source , type_r, item, reading, market, min_sampling, max_sampling, onChange):
        self.rid = rid
        self.report = report._id
        self.report_subject = subject
        self.report_source = source
        self.report_type = type_r
        #self.report_item = item
        self.report_reading = reading
        self.market_context = market
        self.min_sampling = min_sampling
        self.max_sampling = max_sampling
        self.onChange = onChange