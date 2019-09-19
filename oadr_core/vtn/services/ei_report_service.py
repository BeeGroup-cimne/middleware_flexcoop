# -*- coding: utf-8 -*-
from datetime import datetime
from lxml import etree

from oadr_core.oadr_base_service import OadrMessage
from oadr_core.oadr_payloads.oadr_payloads_general import oadrPayload, oadrResponse, NAMESPACES, pretty_print_xml
from oadr_core.oadr_payloads.oadr_payloads_report_service import oadrRegisteredReport, oadrCreatedReport, \
    oadrUpdatedReport, oadrCanceledReport, oadrRegisterReport, oadrUpdateReport, oadrCancelReport, oadrCreateReport
from oadr_core.vtn.models import VEN, MetadataReportSpec, DataPoint, ReportsToSend


def _auto_subsciption_reports(params):
    """Subscribe programatically to reports by adding them to the response of oadrRegisteredReport.
    This is done for registering to some reports when the first metadata is recieved
    """
    # TODO: get reports registerd by a VEN and prepare the subscription to the required ones
    # report_types = [{"reportId": "reportId", "specifierId": "specifierId", "data_points":[{"rid":"a", "reading_type":"Direct Read"},{"rid":"b", "reading_type": "Direct Read"}]}]
    return []


class OadrRegisterReport(OadrMessage):
    def _create_response(self, params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrRegisterReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ is not None else ""
        reportRequestID_ = final_parameters.find(".//ei:reportRequestID", namespaces=NAMESPACES)
        reportRequestID = reportRequestID_.text if reportRequestID_ is not None else None

        # respond
        ven = VEN.find_one({VEN.venID():venID})
        if ven is None:
            content = oadrRegisteredReport("452", "Invalid venID", str(requestID), None, venID)
            return oadrPayload(content)

        ven.remove_reports()
        for r in final_parameters.findall(".//oadr:oadrReport", namespaces=NAMESPACES):
            owned = False
            reportID = r.find('.//ei:eiReportID', namespaces=NAMESPACES).text
            specifierID = r.find('.//ei:reportSpecifierID', namespaces=NAMESPACES).text
            duration = r.find('.//xcal:duration/xcal:duration', namespaces=NAMESPACES).text
            name = r.find('.//ei:reportName', namespaces=NAMESPACES).text
            created = datetime.strptime(r.find('.//ei:createdDateTime', namespaces=NAMESPACES).text[:19],
                                               "%Y-%m-%dT%H:%M:%S")
            report = MetadataReportSpec(ven._id, reportID, specifierID, duration, name, created)
            report.save()
            for d in r.findall('.//oadr:oadrReportDescription', namespaces=NAMESPACES):
                rid= d.find('.//ei:rID', namespaces=NAMESPACES).text
                report_subject_ = d.find(".//power:mrid", namespaces=NAMESPACES)
                report_subject = None #report_subject_.text if report_subject_ else None
                report_source = None
                report_type = d.find(".//ei:reportType", namespaces=NAMESPACES).text
                #report_item = d.find(".//emix:itemBase/*", namespaces=NAMESPACES).tag
                report_reading = d.find(".//ei:readingType", namespaces=NAMESPACES).text
                market_context = d.find(".//emix:marketContext", namespaces=NAMESPACES).text if d.find(".//emix:marketContext", namespaces=NAMESPACES) is not None else None
                min_sampling = d.find(".//oadr:oadrMinPeriod", namespaces=NAMESPACES).text if d.find(".//oadr:oadrMinPeriod", namespaces=NAMESPACES) is not None else None
                max_sampling = d.find(".//oadr:oadrMaxPeriod", namespaces=NAMESPACES).text if d.find(".//oadr:oadrMinPeriod", namespaces=NAMESPACES) is not None else None
                onChange = d.find(".//oadr:oadrOnChange", namespaces=NAMESPACES).text if d.find(".//oadr:oadrOnChange", namespaces=NAMESPACES) is not None else None
                data_point = DataPoint(report._id, rid,  report_type, report_reading, market_context, min_sampling, max_sampling, onChange)
                data_point.save()

        report_types = _auto_subsciption_reports({})
        content = oadrRegisteredReport("200", "OK", str(requestID), report_types, venID)
        return oadrPayload(content)

    def _create_message(self, params):
        # This is an automatic generation, as it will depend on the "database metadata reports owned"
        db_reports = MetadataReportSpec.find({MetadataReportSpec.owned(): True})
        requestID = "0"
        reports = [{"duration":db_r.duration, "eiReportID": db_r.eiReportID,
                    "data_points":[{"rID": dp.rID, "oadrMinPeriod": dp.oadrMinPeriod, "oadrMaxPeriod": dp.oadrMaxPeriod,
                                    "oadrOnChange": dp.oadrOnChange, "marketContext": dp.marketContext,
                                    "reportType": dp.reportType, "readingType": dp.readingType} for dp in DataPoint.find({DataPoint.report():db_r._id})],
                    "reportRequestID": "0", "reportSpecifierID": db_r.specifierID,
                    "reportName": db_r.reportName, "createdDateTime": db_r.createdDateTime.strftime("%Y-%m-%dT%H:%M:%S")
                    } for db_r in db_reports]
        venID = params['venID']

        content = oadrRegisterReport(requestID, requestID, venID, reports)

        return oadrPayload(content)

    def response_callback(self, response):
        params = etree.fromstring(response.text)
        if self._schema_val(params):
            OadrRegisteredReport.register_reports(params)

def register_report(r_request):
    reportSpecifierID = r_request.find(".//ei:reportSpecifierID", namespaces=NAMESPACES).text
    reportRequestID = r_request.find(".//ei:reportRequestID", namespaces=NAMESPACES).text
    granularity = r_request.find(".//xcal:granularity/xcal:duration", namespaces=NAMESPACES).text
    reportBackDuration = r_request.find(".//ei:reportBackDuration/xcal:duration", namespaces=NAMESPACES).text
    relatedDataPoints = []
    for dp in r_request.findall(".//ei:specifierPayload", namespaces=NAMESPACES):
        rID = dp.find(".//ei:rID", namespaces=NAMESPACES).text
        readingType = dp.find(".//ei:readingType", namespaces=NAMESPACES)
        db_dp = DataPoint.find_one({DataPoint.rID(): rID, DataPoint.readingType(): readingType})
        relatedDataPoints.append(db_dp._id)
    report = MetadataReportSpec.find_one(
        {MetadataReportSpec.owned(): True, MetadataReportSpec.specifierID(): reportSpecifierID})
    report_subscription = ReportsToSend(report._id, reportRequestID, granularity, reportBackDuration,
                                        relatedDataPoints)
    report_subscription.save()

class OadrRegisteredReport(OadrMessage):
    @staticmethod
    def register_reports(params):
        final_parameters = params.xpath(".//oadr:oadrRegisteredReport", namespaces=NAMESPACES)[0]
        response_code = final_parameters(".//ei:responseCode", namespaces=NAMESPACES).text
        oadrReportRequest = final_parameters.findall(".//oadr:oadrReportRequest", namespaces=NAMESPACES)
        venID = final_parameters.find(".//ei:venID", namespaces=NAMESPACES).text
        requestID = final_parameters.find(".//pyld:requestID", namespace=NAMESPACES).text
        ven = VEN.find_one({VEN.venID(): venID})
        if response_code == "200" and oadrReportRequest is not None:
            for r_request in oadrReportRequest:
                register_report(r_request)
        content = oadrResponse("200", "OK", str(requestID), venID)
        return content

    def _create_response(self, params):
        content = OadrRegisteredReport.register_reports(params)
        return oadrPayload(content)


class OadrCreatedReport(OadrMessage):
    @staticmethod
    def created_report(params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrCreatedReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        pending_reports = final_parameters.find(".//oadr:oadrPendingReports", namespaces=NAMESPACES)
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ is not None else ""

        # respond

        ven = VEN.find_one({VEN.venID():venID})
        if ven is None:
            content = oadrResponse("452", "Invalid venID", str(requestID), venID)
            return oadrPayload(content)
        #TODO: do watever with pending reports
        content = oadrResponse("200", "OK", str(requestID), venID)
        return content

    def _create_response(self, params):
        content = OadrCreatedReport.created_report(params)
        return oadrPayload(content)

    def _create_message(self, params):
        venID = params['venID']
        ven = VEN.find_one({VEN.venID:venID})
        pending_reports = []
        for rep in MetadataReportSpec.find({MetadataReportSpec.ven: ven._id}):
            pending_reports.extend([r.reportRequestID for r in ReportsToSend.find({ReportsToSend.report:rep._id})])
        content = oadrCreatedReport("200", "OK", "10", pending_reports, "0")
        return oadrPayload(content)

class OadrUpdateReport(OadrMessage):
    def _create_response(self, params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrUpdateReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ is not None else ""
        ven = VEN.find_one({VEN.venID(): venID})
        if ven is None:
            content = oadrUpdatedReport("452", "Invalid venID", str(requestID), None, venID)
            return oadrPayload(content)
        # TODO: Process report as expected
        reports = final_parameters.findall(".//oadr:oadrReport", namespaces=NAMESPACES)
        for report in reports:
            print(report)
            # if report['xcal:dtstart']:
            #     print(report['xcal:dtstart'])
            # if report['xcal:duration']:
            #     print['duration']
            # reportId = report['ei:eiReportID']
            # specifierId = report['oadr:oadrReposrtSpecifierID']
            # for interval in report['strm:intervals']['ei:interval']:
            #     timestamp = interval['xcal:dtstart']
            #     duration = interval['xcal:duration']
            #     value = interval['reportPayload']['payloadFloat']
            #     rid = interval['reportPayload']['ei:rid']
            #     print("Data from {}: time: {}, duration: {}, value: {}")
        content = oadrUpdatedReport("200", "OK", str(requestID), None, venID)

        return oadrPayload(content)

    def _create_message(self, params):
        venID = params['venID']
        requestID = params['requestID']
        reports_dic=[]
        for rep in ReportsToSend.find({}):
            # TODO Get data from report
            if rep.canceled:
                rep.delete()
        content = oadrUpdateReport(requestID, reports_dic, venID)
        return oadrPayload(content)


class OadrCreateReport(OadrMessage):
    def _create_response(self, params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrCreateReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ is not None else ""
        ven = VEN.find_one({VEN.venID(): venID})
        # respond
        if ven is None:
            content = oadrCreatedReport("452", "Invalid venID", str(requestID), None, venID)
            return oadrPayload(content)

        for r_request in final_parameters.findall(".//oadr:oadrReportRequest", namespaces=NAMESPACES):
            register_report(r_request)

        content = oadrCreatedReport("200", "OK", str(requestID), None, venID)
        return oadrPayload(content)

    def _create_message(self, params):
        # get pending reports
        report_types=params['report_types']
        requestID = params['requestID']
        venID = params['venID']
        content = oadrCreateReport(requestID, report_types, venID)
        return oadrPayload(content)

    def response_callback(self, params):
        OadrCreatedReport.created_report(params)

class OadrCancelReport(OadrMessage):
    def _create_response(self, params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrCancelReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text
        report_to_follow = final_parameters.find(".//pyld:reportToFollow", namespaces=NAMESPACES)
        report_to_follow = True if report_to_follow == 'true' else False
        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ is not None else ""

        # respond
        ven = VEN.find_one({VEN.venID(): venID})
        if ven is None:
            content = oadrCanceledReport("452", "Invalid venID", str(requestID), None, venID)
            return oadrPayload(content)

        for report_request in final_parameters.find(".//ei:reportRequestID", namespaces=NAMESPACES):
            report = ReportsToSend.find_one({ReportsToSend.reportRequestID:report_request})
            if report_to_follow:
                report.canceled=True
                report.save()
            else:
                report.delete()
        pending_reports = []
        for rep in MetadataReportSpec.find({MetadataReportSpec.ven: ven._id}):
            pending_reports.extend([r.reportRequestID for r in ReportsToSend.find({ReportsToSend.report: rep._id})])
        content = oadrCanceledReport("200", "OK", str(requestID), pending_reports, venID)
        return oadrPayload(content)

    def _create_message(self, params):
        cancel_report = params['cancelReport']
        requestID = params['requestID']
        venID = params['venID']
        followUp = 'true' if params['followUp'] else 'false'
        content = oadrCancelReport(cancel_report, requestID, venID, followUp)
        return oadrPayload(content)


"""    
#Query Registration
from lxml import etree
from oadr_core.vtn.services.ei_report_service import OadrRegisterReport
from oadr_core.oadr_payloads.oadr_payloads_general import pretty_print_xml
responder = OadrRegisterReport()
xml_t = etree.parse(open('oadr_core/oadr_xml_example/ei_report_service/oadr_register_report.xml'))
response = responder.respond(xml_t)
pretty_print_xml(response)

"""