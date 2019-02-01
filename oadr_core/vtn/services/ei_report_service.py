# -*- coding: utf-8 -*-
from oadr_core.oadr_base_service import OadrMessage
from oadr_core.oadr_payloads.oadr_payloads_general import oadrPayload, oadrResponse, NAMESPACES
from oadr_core.oadr_payloads.oadr_payloads_report_service import oadrRegisteredReport, oadrCreatedReport, \
    oadrUpdatedReport, oadrCanceledReport, oadrRegisterReport, oadrUpdateReport, oadrCancelReport, oadrCreateReport
from oadr_core.vtn.models import VEN


class OadrRegisterReport(OadrMessage):
    def _create_response(self, params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrRegisterReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ else None
        reportRequestID_ = final_parameters.find(".//ei:reportRequestID", namespaces=NAMESPACES)
        reportRequestID = reportRequestID_.text if reportRequestID_ else None

        # respond
        if venID:
            ven = VEN.query.filter(VEN.venID == venID).first()
        else:
            content = oadrRegisteredReport("452", "Invalid venID", str(requestID), None, None, venID)
            return oadrPayload(content)
        # TODO: check report types and prepare subscription to them
        reportRequestList = []
        reportSpecifierList = []
        for r in final_parameters['oadr:oadrReport']:
            report_spec = r['ei:reportSpecifierID']
            for d in r['oadr:oadrReportDescription']:
                print(d['ei:rID'])
            reportRequestList.append(reportRequestID)
            reportSpecifierList.append(report_spec)

        content = oadrRegisteredReport("200", "OK", str(requestID), reportRequestList, reportSpecifierList, venID)
        return oadrPayload(content)

    def send(self, params):
        # TODO: obtain the reports types we can crate
        requestID = 0 # generate requests id
        reports = [{"type": "TELEMETRY_USAGE", "specifierID": "RP_222", "reportID": "ID_222", "duration":"PT1",
                    "datapoins":{"id": "m3", "data_soucrce": "", "itembase": "", "min_period": "PT01",
                    "max_period": "PT01", "market_context": "the market context"}
                    }]
        venID = 0 #get venID to send
        content = oadrRegisterReport(requestID, requestID, venID, reports)
        return oadrPayload(content)


class OadrCreatedReport(OadrMessage):
    def _create_response(self, params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrCreatedReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        pending_reports = final_parameters.find(".//oadr:oadrPendingReports", namespaces=NAMESPACES)
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ else None

        # respond
        if venID:
            ven = VEN.query.filter(VEN.venID == venID).first()
        else:
            content = oadrResponse("452", "Invalid venID", str(requestID), venID)
            return oadrPayload(content)
        #TODO: do watever with pending reports
        content = oadrResponse("200", "OK", str(requestID), venID)
        return oadrPayload(content)

    def send(self, params):
        # TODO: get pending reports
        pending_reports = []
        content = oadrCreatedReport(200, "OK", 10, pending_reports, 0)
        return oadrPayload(content)

class OadrUpdateReport(OadrMessage):
    def _create_response(self, params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrUpdateReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ else None

        #respond
        if venID:
            ven = VEN.query.filter(VEN.venID == venID).first()
        else:
            content = oadrUpdatedReport("452", "Invalid venID", str(requestID), None, venID)
            return oadrPayload(content)
        # TODO: Process report as expected
        reports = final_parameters.findall(".//oadr:oadrReport", namespaces=NAMESPACES)
        for report in reports:
            if report['xcal:dtstart']:
                print(report['xcal:dtstart'])
            if report['xcal:duration']:
                print['duration']
            reportId = report['ei:eiReportID']
            specifierId = report['oadr:oadrReposrtSpecifierID']
            for interval in report['strm:intervals']['ei:interval']:
                timestamp = interval['xcal:dtstart']
                duration = interval['xcal:duration']
                value = interval['reportPayload']['payloadFloat']
                rid = interval['reportPayload']['ei:rid']
                print("Data from {}: time: {}, duration: {}, value: {}")
        content = oadrUpdatedReport("200", "OK", str(requestID), None, venID)
        return oadrPayload(content)

    def send(self, params):
        # TODO Get data for report
        content = oadrUpdateReport(0, [], 0)
        return oadrPayload(content)


class OadrCreateReport(OadrMessage):
    def _create_response(self, params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrCreateReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ else None

        # respond
        if venID:
            ven = VEN.query.filter(VEN.venID == venID).first()
        else:
            content = oadrUpdatedReport("452", "Invalid venID", str(requestID), None, venID)
            return oadrPayload(content)
        for request in final_parameters.findall(".//oadr:oadr_report_requests", namespaces=NAMESPACES):
            print(request)

        # TODO: Process and register the creation of a report VEN REQUESTS A REPORT
        content = oadrCreatedReport("200", "OK", str(requestID), None, venID)
        return oadrPayload(content)

    def send(self, params):
        # get pending reports
        content = oadrCreateReport(0, [], [], 0)
        return oadrPayload(content)


class OadrCancelReport(OadrMessage):
    def _create_response(self, params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrCreateReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text
        report_to_follow = final_parameters.find(".//pyld:reportToFollow", namespaces=NAMESPACES)
        report_to_follow = True if report_to_follow == 'true' else False
        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ else None

        # respond
        if venID:
            ven = VEN.query.filter(VEN.venID == venID).first()
        else:
            content = oadrCanceledReport("452", "Invalid venID", str(requestID), None, venID)
            return oadrPayload(content)

        for report_request in final_parameters.find(".//ei:reportRequestID", namespaces=NAMESPACES):
            print(report_request)

        # TODO: Cancel the REPORT generation and sending, get pending reports for response
        content = oadrCanceledReport("200", "OK", str(requestID), None, venID)
        return oadrPayload(content)

    def send(self, params):
        cancel_report = []
        content = oadrCancelReport(cancel_report, 0)
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