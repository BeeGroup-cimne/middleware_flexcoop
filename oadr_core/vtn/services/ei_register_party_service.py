# -*- coding: utf-8 -*-
from lxml import etree

from oadr_core.exceptions import InvalidVenException
from oadr_core.oadr_base_service import OadrMessage
from oadr_core.oadr_payloads.oadr_payloads_general import oadrPayload, NAMESPACES, oadrResponse
from oadr_core.oadr_payloads.oadr_payloads_register_service import oadrCreatedPartyRegistration, \
    oadrCanceledPartyRegistration, oadrRequestReregistration, oadrCancelPartyRegistration
from project_customization.base_customization import project_configuration


class OadrQueryRegistration(OadrMessage):
    def _create_response(self, params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrQueryRegistration", namespaces=NAMESPACES)[0]
        # Mandatory_parameters
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text
        # Optional parameters
        # """"""

        #respond
        content = oadrCreatedPartyRegistration("200", "OK", str(requestID), None, None, str(project_configuration.VTN_ID),
                                               project_configuration.profiles, str(project_configuration.poll_freq),
                                               project_configuration.specific_info, project_configuration.extensions)
        return oadrPayload(content)



class OadrCreatePartyRegistration(OadrMessage):
    def _create_response(self, params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrCreatePartyRegistration", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text
        oadrProfileName = final_parameters.find(".//oadr:oadrProfileName", namespaces=NAMESPACES).text
        oadrTransportName = final_parameters.find(".//oadr:oadrTransportName", namespaces=NAMESPACES).text
        oadrReportOnly = final_parameters.find(".//oadr:oadrReportOnly", namespaces=NAMESPACES).text
        oadrReportOnly = True if oadrReportOnly == 'true' else False
        oadrXmlSignature = final_parameters.find(".//oadr:oadrXmlSignature", namespaces=NAMESPACES).text
        oadrXmlSignature = True if oadrXmlSignature == 'true' else False

        # Optional parameters
        registrationID_ = final_parameters.find(".//ei:registrationID", namespaces=NAMESPACES)
        registrationID = registrationID_.text if registrationID_ is not None else None
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ is not None else None
        oadrTransportAddress_ = final_parameters.find(".//oadr:oadrTransportAddress", namespaces=NAMESPACES)
        oadrTransportAddress = oadrTransportAddress_.text if oadrTransportAddress_ is not None else None
        oadrVenName_ = final_parameters.find(".//oadr:oadrVenName", namespaces=NAMESPACES)
        oadrVenName = oadrVenName_.text if oadrVenName_ is not None else None

        oadrHttpPullModel_ = final_parameters.find(".//oadr:oadrHttpPullModel", namespaces=NAMESPACES)
        oadrHttpPullModel = oadrHttpPullModel_.text if oadrHttpPullModel_ is not None else None
        oadrHttpPullModel = True if oadrHttpPullModel == 'true' else False

        # respond
        try:
            code, description, registrationID, venID = project_configuration.on_OadrCreatePartyRegistration_recieve(requestID, oadrProfileName, oadrTransportName, oadrReportOnly, oadrXmlSignature, registrationID, venID, oadrTransportAddress, oadrVenName, oadrHttpPullModel)
        except InvalidVenException as e:
            code = e.code
            description = e.description

        content = oadrCreatedPartyRegistration(code, description, str(requestID), str(registrationID), str(venID),
                                               str(project_configuration.VTN_ID), project_configuration.profiles,
                                               str(project_configuration.poll_freq), project_configuration.specific_info,
                                               project_configuration.extensions)
        return oadrPayload(content)


class OadrCancelPartyRegistration(OadrMessage):
    def _create_response(self, params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrCancelPartyRegistration", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text
        registrationID = final_parameters.find(".//ei:registrationID", namespaces=NAMESPACES).text

        # Optional parameters
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ is not None else ""

        # respond
        try:
            code, description = project_configuration.on_OadrCancelPartyRegistration_recieve(requestID, registrationID, venID)
        except InvalidVenException as e:
            code = e.code
            description = e.description

        content = oadrCanceledPartyRegistration(code, description, str(requestID), str(registrationID), str(venID))
        return oadrPayload(content)

    def _create_message(self, params):
        """ this will delete the vtn and all information without wainting for the response. We can't rely on the VEN to
        do it wrong and never be able to get rid of it"""
        registrationID = params['registrationID']
        requestID = params['requestID']
        venID = params['venID']
        project_configuration.on_OadrCancelPartyRegistration_recieve(registrationID, requestID, venID)
        content = oadrCancelPartyRegistration(registrationID, requestID, venID)
        return oadrPayload(content)

    def response_callback(self, response):
        response = etree.fromstring(response.text)
        if self._schema_val(response):
            final_parameters = response.xpath(".//oadr:oadrCancelPartyRegistration", namespaces=NAMESPACES)[0]
            # TODO: set the parameters to the function
            project_configuration.on_OadrCancelPartyRegistration_response()


class OadrCanceledPartyRegistration(OadrMessage):
    def _create_response(self, params):
        final_parameters = params.xpath(".//oadr:oadrCanceledPartyRegistration", namespaces=NAMESPACES)[0]
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text
        venID = final_parameters.find(".//ei:venID", namespaces=NAMESPACES).text
        code, description = project_configuration.on_OadrCanceledPartyRegistration_recieve(requestID, venID)
        content = oadrResponse(code, description, requestID, venID)
        return oadrPayload(content)


class OadrRequestReregistration(OadrMessage):
    def _create_message(self, params):
        # Mandatory parameters
        venID = params['venID']
        project_configuration.on_OadrRequestReregistration_send(venID)
        content = oadrRequestReregistration(venID)
        return oadrPayload(content)

    def response_callback(self, response):
        response = etree.fromstring(response.text)
        if self._schema_val(response):
            final_parameters = response.xpath(".//oadr:oadrResponse", namespaces=NAMESPACES)[0]
            # TODO: set the parameters to the function
            project_configuration.on_OadrRequestReregistration_response()

"""
#Query Registration
from lxml import etree
from oadr_core.vtn.services.ei_register_party_service import OadrQueryRegistration
from oadr_core.oadr_payloads.oadr_payloads_general import pretty_print_xml
responder = OadrQueryRegistration()
xml_t = etree.parse(open('oadr_core/oadr_xml_example/ei_register_service/oadrQueryRegistration.xml'))
response = responder.respond(xml_t)
pretty_print_xml(response)

#Registration:
from lxml import etree
from oadr_core.vtn.services.ei_register_party_service import OadrCreatePartyRegistration
from oadr_core.oadr_payloads.oadr_payloads_general import pretty_print_xml
responder = OadrCreatePartyRegistration()
xml_t = etree.parse(open('oadr_core/oadr_xml_example/ei_register_service/oadrCreatePartyRegistration.xml'))
response = responder.respond(xml_t)
pretty_print_xml(response)

#Cancel Registration
from lxml import etree
from oadr_core.vtn.services.ei_register_party_service import OadrCancelPartyRegistration
from oadr_core.oadr_payloads.oadr_payloads_general import pretty_print_xml
responder = OadrCancelPartyRegistration()
xml_t = etree.parse(open('oadr_core/oadr_xml_example/ei_register_service/cancel_registration.xml'))
response = responder.respond(xml_t)
pretty_print_xml(response)

"""