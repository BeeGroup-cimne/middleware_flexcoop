from sqlalchemy import Column, Integer, String, Boolean, DateTime
from kernel.database import Base

class VEN(Base):
    __tablename__ = 'ven'
    venID = Column(Integer, primary_key=True)
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

