import traceback
from datetime import datetime

from oadr_core.exceptions import InvalidVenException, InvalidReportException, InvalidResponseException
from oadr_core.oadr_payloads.oadr_payloads_general import NAMESPACES
from project_customization.flexcoop.models import VEN, MetadataReports, DataPoint, ReportsToSend
from project_customization.flexcoop.reports.metadata_telemetry_status import MetadataTelemetryStatusReport
from project_customization.flexcoop.reports.metadata_telemetry_usage import MetadataTelemetryUsageReport
from project_customization.flexcoop.reports.telemetry_usage import TelemetryUsageReport


class FlexcoopCustomization():
    profiles = {'2.0b': ['simpleHttp', 'xmpp'], '2.0a': ['simpleHttp', 'xmpp']}
    VTN_ID = 1
    poll_freq = "P3Y6M4DT12H30M5S"
    specific_info = {}  # {"EiEvent":["haha",10]}
    extensions = {}  # {"extension1":["haha",10]}
    reports_to_subscribe = ["Ligth", "HVAC", "DHW"]

    available_reports = {
        "TELEMETRY_USAGE": TelemetryUsageReport()
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
                ven = VEN.find_one({VEN.venID(): venID})
                if ven:
                    raise InvalidVenException()

            ven = VEN(venID, registrationID, oadrProfileName, oadrTransportName, oadrTransportAddress,
                      oadrReportOnly, oadrXmlSignature, oadrVenName, oadrHttpPullModel)
            ven.registrationID = str(ven.venID)
        else:
            ven = VEN.find_one({VEN.registrationID():registrationID})
            if not ven or str(ven.venID) != venID:
                raise InvalidVenException()

        # save info of new ven
        ven.oadrProfileName = oadrProfileName
        ven.oadrTransportName = oadrTransportName
        ven.oadrTransportAddress = oadrTransportAddress
        ven.oadrReportOnly = oadrReportOnly
        ven.oadrXmlSignature = oadrXmlSignature
        ven.oadrVenName = oadrVenName
        ven.oadrHttpPullModel = oadrHttpPullModel
        ven.save()

        return "200", "OK", ven.registrationID, ven.venID

    def on_OadrCancelPartyRegistration_recieved(self, requestID, registrationID, venID):

        ven = VEN.find_one({VEN.registrationID(): registrationID})
        if str(ven.venID) != venID:
            raise InvalidVenException()
        ven.remove_reports()
        ven.delete()
        return "200", "OK"

    def on_OadrCancelPartyRegistration_send(self, registrationID, requestID, venID):
        ven = VEN.find_one({VEN.registrationID(): registrationID})
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
    def auto_subsciption_reports(self):
        """Subscribe programatically to reports by adding them to the response of oadrRegisteredReport.
         This is done for registering to some reports when the first metadata is recieved
         """
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
            db_dp = DataPoint.find_one({DataPoint.rID(): rID, DataPoint.readingType(): readingType})
            relatedDataPoints.append(db_dp._id)
        report = MetadataReports.find_one(
            {MetadataReports.owned(): True, MetadataReports.specifierID(): reportSpecifierID})
        report_subscription = ReportsToSend(report._id, reportRequestID, granularity, reportBackDuration,
                                            relatedDataPoints)
        report_subscription.save()

    def created_report(self, pending_reports):
        #TODO: do watever with pending reports
        return "200", "OK"


    def on_OadrRegisterReport_recieved(self, requestID, venID, reports_payloads):
        ven = VEN.find_one({VEN.venID(): venID})


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


        report_types = self.auto_subsciption_reports()
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

        ven = VEN.find_one({VEN.venID(): venID})
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
        ven = VEN.find_one({VEN.venID(): venID})
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
        ven = VEN.find_one({VEN.venID(): venID})
        # respond
        if ven is None:
            raise InvalidVenException
        return self.created_report(pending_reports)

    def on_OadrCreatedReport_recieved(self, venID, pending_reports):
        ven = VEN.find_one({VEN.venID(): venID})
        # respond
        if ven is None:
            raise InvalidVenException
        return self.created_report(pending_reports)

    def on_OadrCreatedReport_send(self, venID):
        ven = VEN.find_one({VEN.venID:venID})
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
        ven = VEN.find_one({VEN.venID(): venID})

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
        ven = VEN.find_one({VEN.venID(): venID})
        if ven is None:
            raise InvalidVenException

        for i in range(0,len(cancel_reports)):
            report_request = cancel_reports[i]
            report = ReportsToSend.find_one({ReportsToSend.reportRequestID: report_request})
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