import threading
import traceback
from datetime import datetime

from flask import request

from oadr_core.exceptions import InvalidVenException, InvalidReportException, InvalidResponseException
from oadr_core.oadr_payloads.oadr_payloads_general import NAMESPACES
from oadr_core.vtn.services.ei_report_service import OadrCreateReport
from project_customization.flexcoop.models import VEN, MetadataReports, DataPoint, ReportsToSend, Device, \
    map_rid_device_id
from project_customization.flexcoop.reports.metadata_telemetry_status import MetadataTelemetryStatusReport
from project_customization.flexcoop.reports.metadata_telemetry_usage import MetadataTelemetryUsageReport
from project_customization.flexcoop.reports.telemetry_status import TelemetryStatusReport
from project_customization.flexcoop.reports.telemetry_usage import TelemetryUsageReport


class FlexcoopCustomization():
    profiles = {'2.0b': ['simpleHttp', 'xmpp'], '2.0a': ['simpleHttp', 'xmpp']}
    VTN_ID = "1"
    poll_freq = "P3Y6M4DT12H30M5S"
    specific_info = {}  # {"EiEvent":["haha",10]}
    extensions = {}  # {"extension1":["haha",10]}
    reports_to_subscribe = ["Ligth", "HVAC", "DHW"]

    available_reports = {
        "TELEMETRY_USAGE": TelemetryUsageReport(),
        "TELEMETRY_STATUS": TelemetryStatusReport()
    }
    metadata_available_reports = {
        "METADATA_TELEMETRY_USAGE": MetadataTelemetryUsageReport(),
        "METADATA_TELEMETRY_STATUS": MetadataTelemetryStatusReport()
    }

    ### EI REGISTER PARTY
    def on_OadrCreatePartyRegistration_recieved(self, requestID, oadrProfileName, oadrTransportName, oadrReportOnly, oadrXmlSignature,
                                                registrationID, venID, oadrTransportAddress, oadrVenName, oadrHttpPullModel):

        # Check for correct ven and registrationID
        if not registrationID:
            if venID:
                ven = VEN.get_ven(venID)
                if ven:
                    raise InvalidVenException()

            ven = VEN(venID, registrationID, oadrProfileName, oadrTransportName, oadrTransportAddress,
                      oadrReportOnly, oadrXmlSignature, oadrVenName, oadrHttpPullModel)
            ven.registration_id = str(ven.ven_id)
        else:
            tven = VEN.find_one({VEN.registration_id():registrationID})
            ven = VEN.get_ven(tven.ven_id)
            if not ven or str(ven.ven_id) != venID:
                raise InvalidVenException()

        # save info of new ven
        ven.oadr_profile_name = oadrProfileName
        ven.oadr_transport_name = oadrTransportName
        ven.oadr_transport_address = oadrTransportAddress
        ven.oadr_report_only = oadrReportOnly
        ven.oadr_xml_signature = oadrXmlSignature
        ven.oadr_ven_name = oadrVenName
        ven.oadr_http_pull_model = oadrHttpPullModel
        ven.save()

        return "200", "OK", ven.registration_id, ven.ven_id

    def on_OadrCancelPartyRegistration_recieved(self, requestID, registrationID, venID):

        ven = VEN.get_ven(registrationID)
        if str(ven.venID) != venID:
            raise InvalidVenException()
        ven.remove_reports()
        ven.delete()
        return "200", "OK"

    def on_OadrCancelPartyRegistration_send(self, registrationID, requestID, venID):
        ven = VEN.find_one({VEN.registration_id(): registrationID})
        ven.remove_reports()
        ven.delete()

    def on_OadrCancelPartyRegistration_response(self):
        # TODO: see if we have to do something with the response
        print("missing_parameters TODO")

    def on_OadrCanceledPartyRegistration_recieved(self, requestID, venID):
        # As we have removed VEN on sending the cancel, we only need to return OK
        return "200", "OK"

    def on_OadrRequestReregistration_send(self, venID):
        pass

    def on_OadrRequestReregistration_response(self):
        # TODO: see if we have to do something with the response
        print("missing_parameters TODO")

    ### EI REPORT

    def auto_subscription_reports_create(self, venID):
        """ When called sends oadrCreateReport with all the reports recieved by the VEN
        """
        # get all reports from user:
        ven = VEN.find_one({VEN.ven_id(): venID})
        reports = MetadataReports.find({MetadataReports.ven(): ven._id})
        report_data_points = {}
        for report in reports:
            report_data_points[report] = {"data_points": DataPoint.find({DataPoint.report(): report._id}),
                                          "devices": Device.find({Device.report(): report._id})}

        register_reports = []
        for report, data_points in report_data_points.items():
            register_data_points = []
            for data_point in data_points['data_points']:
                for k, v in data_point.reporting_items.items():
                    register_data_points.append((data_point, v['oadr_name'], v['reading_type']))
                    data_point.reporting_items[k]['subscribed'] = True
                    data_point.save()

            for device in data_points['devices']:
                for k, v in device.status.items():
                    register_data_points.append((device, v['oadr_name'], v['reading_type']))
                    device.status[k]['subscribed'] = True
                    device.save()

            report.subscribed = True
            report.save()
            register_reports.append((report, register_data_points))

        if register_reports:
            createReport = OadrCreateReport()
            report_types = [{"reportId": x.ei_report_id,
                             "specifierId": x.specifier_id,
                             "data_points": [
                                 {
                                     'rid': "{}_{}".format(map_rid_device_id.find_one(
                                         {map_rid_device_id.device_id(): d[0].device_id}).rid, d[1]),
                                     'reading_type': d[2]
                                 } for d in y]
                             } for x, y in register_reports]
            params = {
                "requestID": "0",
                "report_types": report_types
            }
            from oadr_core.vtn.server_blueprint import send_message
            response = send_message(createReport, ven, params)
            # TODO do something with the response if required by the protocol. When oadrPoll response will be None


    def auto_subsciption_reports(self, venID):
        """Subscribe programatically to reports by adding them to the response of oadrRegisteredReport.
         This is done for registering to some reports when the first metadata is recieved
         """
       # As a flexcoop special action, start a thread, wait 10 seconds and subscrive to all reports using the createReport
        tp_thread = threading.Timer(10, self.auto_subscription_reports_create, args=(venID))
        tp_thread.start()

        # TODO: get reports registerd by a VEN and prepare the subscription to the required ones

        # report_types = [{"reportId": "reportId", "specifierId": "specifierId", "data_points":[{"rid":"a", "reading_type":"Direct Read"},{"rid":"b", "reading_type": "Direct Read"}]}]
        return []


    def register_report(self, r_request):
        reportSpecifierID = r_request.find(".//ei:reportSpecifierID", namespaces=NAMESPACES).text
        reportRequestID = r_request.find(".//ei:reportRequestID", namespaces=NAMESPACES).text
        granularity = r_request.find(".//xcal:granularity/xcal:duration", namespaces=NAMESPACES).text
        reportBackDuration = r_request.find(".//ei:reportBackDuration/xcal:duration", namespaces=NAMESPACES).text
        relatedDataPoints = []
        for dp in r_request.findall(".//ei:specifierPayload", namespaces=NAMESPACES):
            rID = dp.find(".//ei:rID", namespaces=NAMESPACES).text
            readingType = dp.find(".//ei:readingType", namespaces=NAMESPACES)
            db_dp = DataPoint.find_one({DataPoint.rid(): rID, DataPoint.reading_type(): readingType})
            relatedDataPoints.append(db_dp._id)
        report = MetadataReports.find_one(
            {MetadataReports.owned(): True, MetadataReports.specifier_id(): reportSpecifierID})
        report_subscription = ReportsToSend(report._id, reportRequestID, granularity, reportBackDuration,
                                            relatedDataPoints)
        report_subscription.save()

    def created_report(self, pending_reports):
        #TODO: do watever with pending reports
        return "200", "OK"


    def on_OadrRegisterReport_recieved(self, requestID, venID, reports_payloads):
        ven = VEN.get_ven(venID)


        if ven is None:
            raise InvalidVenException
        ven.remove_reports()
        for r in reports_payloads:
            try:
                type_r = r.find(".//ei:reportName", namespaces=NAMESPACES).text
            except AttributeError as e:
                raise InvalidReportException("Undefinded reportName")
            try:
                report_type = self.metadata_available_reports[type_r]
                report_type.parse(r, ven)
            except KeyError as e:
                raise InvalidReportException("unsuported report {}: {}".format(type_r, e))
            except Exception as e:
                print(traceback.format_exc())
                raise InvalidReportException("error in report")


        report_types = self.auto_subsciption_reports(venID)
        return "200", "OK", report_types

    def on_OadrRegisterReport_send(self, venID ):
        db_reports = MetadataReports.find({MetadataReports.owned(): True})
        requestID = "0"
        reportRequestID = 0
        reports = [{"duration": db_r.duration, "eiReportID": db_r.eiReportID,
                    "data_points": [
                        {"rID": dp.rID, "oadrMinPeriod": dp.oadrMinPeriod, "oadrMaxPeriod": dp.oadrMaxPeriod,
                         "oadrOnChange": dp.oadrOnChange, "marketContext": dp.marketContext,
                         "reportType": dp.reportType, "readingType": dp.readingType} for dp in
                        DataPoint.find({DataPoint.report(): db_r._id})],
                    "reportRequestID": "0", "reportSpecifierID": db_r.specifierID,
                    "reportName": db_r.reportName, "createdDateTime": db_r.createdDateTime.strftime("%Y-%m-%dT%H:%M:%S")
                    } for db_r in db_reports]

        return requestID, reportRequestID, reports

    def on_OadrRegisterReport_response(self, response_code, response_description, venID, oadrReportRequest):

        ven = VEN.get_ven(venID)
        if not ven:
            raise InvalidVenException

        if response_code == "200":
            if oadrReportRequest is not None:
                for r_request in oadrReportRequest:
                    self.register_report(r_request)
        else:
            raise InvalidResponseException(response_code, response_description)

        return "200", "OK", venID

    def on_OadrRegisteredReport_recieved(self, response_code, response_description, venID, oadrReportRequest):
        return self.on_OadrRegisterReport_response(response_code, response_description, venID, oadrReportRequest)


    def on_OadrCreateReport_recieved(self, venID, reportRequests):
        ven = VEN.get_ven(venID)
        # respond
        if ven is None:
            raise InvalidVenException

        for r_request in reportRequests:
            self.register_report(r_request)

        pending_reports = []
        for rep in MetadataReports.find({MetadataReports.ven: ven._id}):
            pending_reports.extend([r.reportRequestID for r in ReportsToSend.find({ReportsToSend.report:rep._id})])

        return "200", "OK", pending_reports

    def on_OadrCreateReport_send(self):
        #TODO IT IS LINKED TO THE WEB UI
        pass

    def on_OadrCreateReport_response(self, venID, pending_reports):
        ven = VEN.get_ven(venID)
        # respond
        if ven is None:
            raise InvalidVenException
        return self.created_report(pending_reports)

    def on_OadrCreatedReport_recieved(self, venID, pending_reports):
        ven = VEN.get_ven(venID)
        # respond
        if ven is None:
            raise InvalidVenException
        return self.created_report(pending_reports)

    def on_OadrCreatedReport_send(self, venID):
        ven = VEN.find_one({VEN.ven_id:venID})
        if ven is None:
            raise InvalidVenException
        pending_reports = []
        for rep in MetadataReports.find({MetadataReports.ven: ven._id}):
            pending_reports.extend([r.reportRequestID for r in ReportsToSend.find({ReportsToSend.report:rep._id})])

        return "200", "OK", pending_reports

    def on_OadrCreatedReport_response(self, responseCode, responseDescription):
        if responseCode != "200":
            raise InvalidResponseException(responseCode, responseDescription)


    def on_OadrUpdateReport_recieved(self, venID, reports):
        ven = VEN.get_ven(venID)

        if ven is None:
            raise InvalidVenException

        for report in reports:
            try:
                type_r = report.find(".//ei:reportName", namespaces=NAMESPACES).text
            except AttributeError as e:
                raise InvalidReportException("Undefinded reportName")
            try:
                report_type = self.available_reports[type_r]
                report_type.parse(report)
                # TODO Report Send to Hypertech
            except Exception as e:
                print(e)
                print("Recieved unsuported report {}".format(type_r))
                raise InvalidReportException("unsuported report {}".format(type_r))
        #TODO: get list of canceled reports
        cancel_reports = None
        return "200", "OK", cancel_reports

    def on_OadrUpdateReport_send(self, venID):
        #TODO: Search for venID
        reports_dic = []
        for rep in ReportsToSend.find({}):
            # TODO Get data from report
            if rep.canceled:
                rep.delete()
        return reports_dic

    def on_OadrUpdateReport_response(self, responseCode, responseDescription, cancelReports):
        if responseCode != "200":
            raise InvalidResponseException(responseCode, responseDescription)
        # TODO: Cancel reports (not mandatory)

    def on_OadrUpdatedReport_recieved(self, responseCode, responseDescription, cancelReports):
        if responseCode != "200":
            raise InvalidResponseException(responseCode, responseDescription)
        # TODO: Cancel reports (not mandatory)
        return "200", "OK"

    def on_OadrCancelReport_recieved(self, venID, cancel_reports, report_to_follow):
        # respond
        ven = VEN.get_ven(venID)
        if ven is None:
            raise InvalidVenException

        for i in range(0,len(cancel_reports)):
            report_request = cancel_reports[i]
            report = ReportsToSend.find_one({ReportsToSend.report_request_id: report_request})
            if report_to_follow[i]:
                report.canceled = True
                report.save()
            else:
                report.delete()
        pending_reports = []
        for rep in MetadataReports.find({MetadataReports.ven: ven._id}):
            pending_reports.extend([r.reportRequestID for r in ReportsToSend.find({ReportsToSend.report: rep._id})])

        return "200", "OK", pending_reports

    def on_OadrCancelReport_response(self, response_code, response_description, pending_reports):
        if response_code != "200":
            raise InvalidResponseException(response_code, response_description)
        # TODO: do watever with pending reports
        return "200", "OK"


    def on_OadrCanceledReport_recieved(self,venID, responseCode, responseDescription, pending_reports):
        if responseCode != "200":
            raise InvalidResponseException(responseCode, responseDescription)
        # TODO: do watever with pending reports
        return "200", "OK"