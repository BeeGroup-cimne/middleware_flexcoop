from datetime import datetime
from lxml import etree

from oadr_core.oadr_payloads.oadr_payloads_general import NAMESPACES
from oadr_core.oadr_payloads.reports.report import OadrReport
from project_customization.flexcoop.models import MetadataReports, DataPoint


class MetadataTelemetryUsageReport(OadrReport):
    report_name = "METADATA_TELEMETRY_USAGE"

    def parse(self, oadrReport, ven, *args, **kwargs):
        eiReportID = oadrReport.find('.//ei:eiReportID', namespaces=NAMESPACES).text
        reportSpecifierID = oadrReport.find('.//ei:reportSpecifierID', namespaces=NAMESPACES).text
        duration = oadrReport.find('.//xcal:duration/xcal:duration', namespaces=NAMESPACES).text
        reportName = oadrReport.find('.//ei:reportName', namespaces=NAMESPACES).text
        createdDateTime = datetime.strptime(oadrReport.find('.//ei:createdDateTime', namespaces=NAMESPACES).text[:19],
                                    "%Y-%m-%dT%H:%M:%S")

        report = MetadataReports(ven._id, eiReportID, reportSpecifierID, duration, reportName, createdDateTime)
        report.save()
        for d in oadrReport.findall('.//oadr:oadrReportDescription', namespaces=NAMESPACES):
            rID = d.find('.//ei:rID', namespaces=NAMESPACES).text
            reportSubject_ = d.find(".//ei:reportSubject/power:endDeviceAsset/power:mrid", namespaces=NAMESPACES)
            reportSubject = reportSubject_.text if reportSubject_ is not None else None

            reportDataSource_ = d.find(".//ei:reportDataSource/ei:resourceID", namespaces=NAMESPACES)
            reportDataSource = reportDataSource_.text if reportDataSource_ is not None else None

            reportType_ = d.find(".//ei:reportType", namespaces=NAMESPACES)
            reportType = reportType_.text

            #get next item after report_type, if it is not readingType we have a itemBase
            reportItem_ = reportType_.getnext()
            reportItem = {}
            if reportItem_.xpath('local-name()') != "readingType":
                for c in reportItem_.getchildren():
                    reportItem[c.xpath('local-name()')] = c.text
            readingType = d.find(".//ei:readingType", namespaces=NAMESPACES).text
            marketContext = d.find(".//emix:marketContext", namespaces=NAMESPACES).text if d.find(
                ".//emix:marketContext", namespaces=NAMESPACES) is not None else None
            minSampling = d.find(".//oadr:oadrMinPeriod", namespaces=NAMESPACES).text if d.find(
                ".//oadr:oadrMinPeriod", namespaces=NAMESPACES) is not None else None
            maxSampling = d.find(".//oadr:oadrMaxPeriod", namespaces=NAMESPACES).text if d.find(
                ".//oadr:oadrMinPeriod", namespaces=NAMESPACES) is not None else None
            onChange = d.find(".//oadr:oadrOnChange", namespaces=NAMESPACES).text if d.find(".//oadr:oadrOnChange",
                                                                                            namespaces=NAMESPACES) is not None else None
            data_point = DataPoint(report._id, rID, reportSubject, reportDataSource, reportType, reportItem, readingType, marketContext, minSampling, maxSampling, onChange)
            data_point.save()

    def create(self, *args, **kwargs):
        pass