import enum
from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Enum
import uuid
from kernel.database import Base

def gen_id():
    return str(uuid.uuid4())[:15]

class VEN(Base):
    __tablename__ = 'ven'
    venID = Column(String(50), primary_key=True, default=gen_id)
    registrationID = Column(String(50), unique=True)
    oadrProfileName = Column(String(10))
    oadrTransportName = Column(String(10))
    oadrTransportAddress = Column(String(500))
    oadrReportOnly = Column(Boolean())
    oadrXmlSignature = Column(Boolean())
    oadrVenName = Column(String(50))
    oadrHttpPullModel = Column(Boolean())

    def __init__(self, venID, registrationID, oadrProfileName, oadrTransportName, oadrTransportAddress,
                 oadrReportOnly, oadrXmlSignature, oadrVenName, oadrHttpPullModel):
        if venID:
            self.venID = venID
        if registrationID:
            self.registrationID = registrationID
        self.oadrProfileName = oadrProfileName
        self.oadrTransportName = oadrTransportName
        self.oadrTransportAddress = oadrTransportAddress
        self.oadrReportOnly = oadrReportOnly
        self.oadrXmlSignature = oadrXmlSignature
        self.oadrVenName = oadrVenName
        self.oadrHttpPullModel = oadrHttpPullModel

    def __repr__(self):
        return '<VEN {}>'.format(self.oadrVenName)


class MetadataReportSpec(Base):
    __tablename__ = 'metadata_report_specification'
    ven = Column(ForeignKey('ven.venID'), nullable=True)
    owned = Column(Boolean)
    reportID = Column(String(30), primary_key=True)
    specifierID = Column(String(30), unique=True)
    duration = Column(String(20))
    name = Column(String(100))
    created = Column(DateTime, default=datetime.utcnow)
    subscribed = Column(Boolean, default=False)

    def __init__(self, venID, owned, reportID, specifierID, duration, name, created):
        self.ven = venID
        self.owned = owned
        self.reportID = reportID
        self.specifierID = specifierID
        self.duration = duration
        self.name = name
        self.created = created


class DataPointItemEnum(enum.Enum):
    current = "current"
    energyApparent = "energyApparent"
    energyReactive = "energyReactive"
    energyReal = "energyReal"
    powerApparent = "powerApparent"
    powerReactive = "powerApparent"
    powerReal = "powerReal"
    voltage = "voltage"
    GBdescription = "GBdescription"
    currency = "currency"


class DataPoint(Base):
    __tablename__ = 'datapoints'
    rid = Column(String, primary_key=True)
    report = Column(ForeignKey('metadata_report_specification.reportID'), primary_key=True)
    report_subject = Column(String(50))
    report_source = Column(String(50))
    report_type = Column(String(50))
    #report_item = Column(Enum(DataPointItemEnum), nullable=True)
    report_reading = Column(String(50))
    market_context = Column(String(50))
    min_sampling = Column(String(50))
    max_sampling = Column(String(50))
    onChange = Column(String(50))

    def __init__(self, rid, report, subject, source , type_r, item, reading, market, min_sampling, max_sampling, onChange):
        self.rid = rid
        self.report = report.reportID
        self.report_subject = subject
        self.report_source = source
        self.report_type = type_r
        #self.report_item = item
        self.report_reading = reading
        self.market_context = market
        self.min_sampling = min_sampling
        self.max_sampling = max_sampling
        self.onChange = onChange