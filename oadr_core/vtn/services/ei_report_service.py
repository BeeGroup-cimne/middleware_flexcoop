# -*- coding: utf-8 -*-
from datetime import datetime
from lxml import etree

from oadr_core.exceptions import InvalidVenException, InvalidResponseException, InvalidReportException
from oadr_core.oadr_base_service import OadrMessage
from oadr_core.oadr_payloads.oadr_payloads_general import oadrPayload, oadrResponse, NAMESPACES
from oadr_core.oadr_payloads.oadr_payloads_report_service import oadrRegisteredReport, oadrCreatedReport, \
    oadrUpdatedReport, oadrCanceledReport, oadrRegisterReport, oadrUpdateReport, oadrCancelReport, oadrCreateReport


class OadrRegisterReport(OadrMessage):
    """Class to deal with Report registration

        create_response: The VEN sends the metadata report with all the reports
        create_message: The VTN sends the metadata report with all the reports (PUSH or PULL)
        response_callback: The VEN PUSH that responds to the "create_message" with the registered_reports if any
    """
    def _create_response(self, params):
        """recieves a RegisterReport"""
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrRegisterReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ is not None else ""

        # respond
        try:
            from project_customization.base_customization import project_configuration
            code, description, report_types = project_configuration.on_OadrRegisterReport_recieved(requestID, venID, final_parameters.findall(".//oadr:oadrReport", namespaces=NAMESPACES))
        except InvalidVenException as e:
            code = e.code
            description = e.description
            report_types = []
        except InvalidReportException as e:
            code = e.code
            description = e.description
            report_types = []
        content = oadrRegisteredReport(code, description, str(requestID), report_types, venID)
        return oadrPayload(content)

    def _create_message(self, params):
        """send a RegisterReport message"""
        # This is an automatic generation, as it will depend on the "database metadata reports owned"
        venID = params['venID']
        from project_customization.base_customization import project_configuration
        requestID, reportRequestID, reports = project_configuration.on_OadrRegisterReport_send(venID)
        content = oadrRegisterReport(requestID, requestID, venID, reports)
        return oadrPayload(content)

    def response_callback(self, response):
        """response to RegisterReport message sent"""
        params = etree.fromstring(response.text)
        if self._schema_val(params):
            final_parameters = params.xpath(".//oadr:oadrRegisteredReport", namespaces=NAMESPACES)[0]
            response_code = final_parameters(".//ei:responseCode", namespaces=NAMESPACES).text
            responseDescription = final_parameters(".//ei:responseDescription", namespaces=NAMESPACES).text
            oadrReportRequest = final_parameters.findall(".//oadr:oadrReportRequest", namespaces=NAMESPACES)
            venID = final_parameters.find(".//ei:venID", namespaces=NAMESPACES).text
            requestID = final_parameters.find(".//pyld:requestID", namespace=NAMESPACES).text
            try:
                from project_customization.base_customization import project_configuration
                project_configuration.on_OadrRegisterReport_response(response_code, responseDescription, venID, oadrReportRequest)
            except InvalidVenException as e:
                print("Recieved an invalid VEN")
            except InvalidResponseException as e:
                print("Invalid response with code {} due to {}".format(e.code, e.description))


class OadrRegisteredReport(OadrMessage):
    """Class to deal with the report registration
        create_response: response of VEN with PULL method that recieved the OadrRegisterReport message
    """
    def _create_response(self, params):
        """response to RegisteredReport message"""
        final_parameters = params.xpath(".//oadr:oadrRegisteredReport", namespaces=NAMESPACES)[0]
        response_code = final_parameters(".//ei:responseCode", namespaces=NAMESPACES).text
        responseDescription = final_parameters(".//ei:responseDescription", namespaces=NAMESPACES).text
        oadrReportRequest = final_parameters.findall(".//oadr:oadrReportRequest", namespaces=NAMESPACES)
        venID = final_parameters.find(".//ei:venID", namespaces=NAMESPACES).text
        requestID = final_parameters.find(".//pyld:requestID", namespace=NAMESPACES).text
        try:
            from project_customization.base_customization import project_configuration

            code, description = project_configuration.on_OadrRegisteredReport_recieved(response_code, responseDescription, venID,
                                                                 oadrReportRequest)
        except InvalidVenException as e:
            code = e.code
            description = e.description
            print("Recieved an invalid VEN")

        except InvalidResponseException as e:
            code = e.code
            description = e.description
            print("Invalid response with code {} due to {}".format(e.code, e.description))

        content = oadrResponse(code, description, str(requestID), venID)
        return oadrPayload(content)


class OadrCreatedReport(OadrMessage):
    """
    Class to deal with the report subscription
    create_response: the VTN recieved report subscription by the VEN after a OadrCreateReport send message via PULL
    create_message: the VTN subscribes to reports in the VEN (PUSH and PULL)
    """
    def _create_response(self, params):
        """"""
        final_parameters = params.xpath(".//oadr:oadrCreatedReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        pending_reports = final_parameters.findall(".//oadr:oadrPendingReports", namespaces=NAMESPACES)
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ is not None else ""

        try:
            from project_customization.base_customization import project_configuration

            code, description = project_configuration.on_OadrCreatedReport_recieved(venID, pending_reports)
        except InvalidVenException as e:
            code = e.code
            description = e.description

        content = oadrResponse(code, description, str(requestID), venID)
        return oadrPayload(content)

    def _create_message(self, params):
        venID = params['venID']
        # TODO: define request ID
        requestID = 0
        try:
            from project_customization.base_customization import project_configuration

            code, description, pending_reports = project_configuration.on_OadrCreatedReport_send(venID)
        except InvalidVenException as e:
            code = e.code
            description = e.description
            pending_reports = None

        content = oadrCreatedReport(code, description, requestID, pending_reports, venID)
        return oadrPayload(content)

    def response_callback(self, params):
        """response to oadrResponse recieved"""
        final_parameters = params.xpath(".//oadr:oadrResponse", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        responseCode = final_parameters.find(".//ei:responseCode", namespaces=NAMESPACES).text
        responseDescription = final_parameters.find(".//ei:responseDescription", namespaces=NAMESPACES).text

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ is not None else ""
        try:
            from project_customization.base_customization import project_configuration

            project_configuration.on_OadrCreatedReport_response(venID, responseCode, responseDescription)
        except InvalidResponseException as e:
            print(e)

class OadrUpdateReport(OadrMessage):
    def _create_response(self, params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrUpdateReport", namespaces=NAMESPACES)[0]
        # Mandatory parameters
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ is not None else ""
        reports = final_parameters.findall(".//oadr:oadrReport", namespaces=NAMESPACES)
        try:
            from project_customization.base_customization import project_configuration

            code, description, cancel_reports = project_configuration.on_OadrUpdateReport_recieved(venID, reports)
        except InvalidVenException as e:
            code = e.code
            description = e.description
            cancel_reports = None
        except InvalidReportException as e:
            code = e.code
            description = e.description
            cancel_reports = None

        content = oadrUpdatedReport(code, description, str(requestID), cancel_reports, venID)
        return oadrPayload(content)

    def _create_message(self, params):
        venID = params['venID']
        requestID = params['requestID']
        from project_customization.base_customization import project_configuration

        reports_dic = project_configuration.on_OadrUpdateReport_send(venID)
        content = oadrUpdateReport(requestID, reports_dic, venID)
        return oadrPayload(content)

    def response_callback(self, params):
        """response to update report sent"""
        final_parameters = params.xpath(".//oadr:oadrUpdatedReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        responseCode = final_parameters.find(".//ei:responseCode", namespaces=NAMESPACES).text
        responseDescription = final_parameters.find(".//ei:responseDescription", namespaces=NAMESPACES).text

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ is not None else ""
        cancelReports = final_parameters.findall(".//ei:reportRequestID")
        try:
            from project_customization.base_customization import project_configuration

            project_configuration.on_OadrUpdateReport_response(venID, responseCode, responseDescription, cancelReports)
        except InvalidResponseException as e:
            print(e)

class OadrUpdatedReport(OadrMessage):
    def _create_response(self, params):
        final_parameters = params.xpath(".//oadr:oadrUpdatedReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        responseCode = final_parameters.find(".//ei:responseCode", namespaces=NAMESPACES).text
        responseDescription = final_parameters.find(".//ei:responseDescription", namespaces=NAMESPACES).text
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text
        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ is not None else ""
        cancelReports = final_parameters.findall(".//ei:reportRequestID")
        try:
            from project_customization.base_customization import project_configuration

            code, description = project_configuration.on_OadrUpdatedReport_recieved(venID, responseCode, responseDescription, cancelReports)
        except InvalidResponseException as e:
            print(e)
        content = oadrResponse(code, description, str(requestID), venID)
        return oadrPayload(content)

class OadrCreateReport(OadrMessage):
    """
    Class to manage the reports to subscribe
    create_response: The VEN requests some reports to the VTN
    create_message: The VTN registers to some reports in the VEN (PUSH or PULL)
    response_callback: The VEN PUSH responds the "create_message" with a oadrCreatedReport
    """
    def _create_response(self, params):
        """response to CreateReport message"""
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrCreateReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ is not None else ""

        try:
            from project_customization.base_customization import project_configuration

            code, description, pending_reports = project_configuration.on_OadrCreateReport_recieved(venID, final_parameters.findall(".//oadr:oadrReportRequest", namespaces=NAMESPACES))
        except InvalidVenException as e:
            code = e.code
            description = e.description
            pending_reports = None

        content = oadrCreatedReport(code, description, str(requestID), pending_reports, venID)
        return oadrPayload(content)

    def _create_message(self, params):
        """send a CreateReport message"""
        # get pending reports
        report_types=params['report_types']
        requestID = params['requestID']
        venID = params['venID']
        content = oadrCreateReport(requestID, report_types, venID)
        return oadrPayload(content)

    def response_callback(self, params):
        """response to CreatedReport recieved"""
        final_parameters = params.xpath(".//oadr:oadrCreatedReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        pending_reports = final_parameters.findall(".//oadr:oadrPendingReports", namespaces=NAMESPACES)
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ is not None else ""

        try:
            from project_customization.base_customization import project_configuration

            code, description = project_configuration.on_OadrCreateReport_response(venID, pending_reports)
        except InvalidVenException as e:
            code = e.code
            description = e.description


class OadrCancelReport(OadrMessage):
    def _create_response(self, params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrCancelReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text
        report_to_follow = final_parameters.findall(".//pyld:reportToFollow", namespaces=NAMESPACES)
        report_to_follow = [True if r =="true" else False for r in report_to_follow]
        cancel_reports = [x.text for x in final_parameters.findall(".//ei:reportRequestID", namespaces=NAMESPACES)]

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ is not None else ""

        # respond
        from project_customization.base_customization import project_configuration

        code, description, pending_reports = project_configuration.on_OadrCancelReport_recieved(venID, cancel_reports, report_to_follow)
        content = oadrCanceledReport(code, description, str(requestID), pending_reports, venID)
        return oadrPayload(content)

    def _create_message(self, params):
        cancel_report = params['cancelReport']
        requestID = params['requestID']
        venID = params['venID']
        followUp = 'true' if params['followUp'] else 'false'
        content = oadrCancelReport(cancel_report, requestID, venID, followUp)
        return oadrPayload(content)

    def response_callback(self, params):
        """response to CancelReport recieved"""
        final_parameters = params.xpath(".//oadr:oadrCanceledReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        pending_reports = final_parameters.findall(".//oadr:oadrPendingReports", namespaces=NAMESPACES)
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text
        response_code = final_parameters(".//ei:responseCode", namespaces=NAMESPACES).text
        responseDescription = final_parameters(".//ei:responseDescription", namespaces=NAMESPACES).text

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ is not None else ""

        try:
            from project_customization.base_customization import project_configuration

            code, description = project_configuration.on_OadrCancelReport_response(response_code, responseDescription, pending_reports)
        except InvalidResponseException as e:
            code = e.code
            description = e.description
            print(description)


class OadrCanceledReport(OadrMessage):
    def _create_response(self, params):
        final_parameters = params.xpath(".//oadr:oadrCanceledReport", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        pending_reports = final_parameters.findall(".//oadr:oadrPendingReports", namespaces=NAMESPACES)
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text
        responseCode = final_parameters(".//ei:responseCode", namespaces=NAMESPACES).text
        responseDescription = final_parameters(".//ei:responseDescription", namespaces=NAMESPACES).text

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ is not None else ""

        try:
            from project_customization.base_customization import project_configuration

            code, description = project_configuration.on_OadrCanceledReport_recieved(venID, responseCode, responseDescription, pending_reports)
        except InvalidResponseException as e:
            code = e.code
            description = e.description
        content = oadrResponse(code, description, str(requestID), venID)
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