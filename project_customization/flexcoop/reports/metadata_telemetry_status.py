from datetime import datetime

from oadr_core.oadr_payloads.oadr_payloads_general import NAMESPACES
from oadr_core.oadr_payloads.reports.report import OadrReport
from project_customization.flexcoop.models import MetadataReports, DataPoint, Device, map_rid_device_id
from project_customization.flexcoop.utils import parse_rid, status_mapping


class MetadataTelemetryStatusReport(OadrReport):
    report_name = "METADATA_TELEMETRY_STATUS"

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
            phisical_device, pdn, groupID, spaces, load, ln, metric = parse_rid(rID)
            status_item = {
                status_mapping[metric]: {
                    "value": None,
                    "report_type": reportType,
                    "units": reportItem,
                    "reading_type": readingType,
                    "market_context": marketContext,
                    "min_period": minSampling,
                    "max_sampling": maxSampling,
                    "on_change": onChange,
                    "subscribed": False,
                    "oadr_name": metric
                }
            }
            deviceID = map_rid_device_id.get_or_create_deviceID(rID)
            device = Device.get_or_create(report._id, deviceID, load, spaces, reportSubject, reportDataSource, status_item)
            device.save()


    def create(self, *args, **kwargs):
        pass