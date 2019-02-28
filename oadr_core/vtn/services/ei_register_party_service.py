# -*- coding: utf-8 -*-
from lxml import etree
from oadr_core.oadr_base_service import OadrMessage
from oadr_core.oadr_payloads.oadr_payloads_general import oadrPayload, NAMESPACES, oadrResponse
from oadr_core.oadr_payloads.oadr_payloads_register_service import oadrCreatedPartyRegistration, \
    oadrCanceledPartyRegistration, oadrRequestReregistration, oadrCancelPartyRegistration
from oadr_core.vtn.configuration import *
from oadr_core.vtn.models import VEN, DataPoint, MetadataReportSpec


class OadrQueryRegistration(OadrMessage):
    def _create_response(self, params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrQueryRegistration", namespaces=NAMESPACES)[0]
        # Mandatory_parameters
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text
        # Optional parameters
        # """"""

        #respond
        content = oadrCreatedPartyRegistration("200", "OK", str(requestID), None, None, str(VTN_ID),
                                            profiles, str(poll_freq), specific_info, extensions)
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
        if not registrationID:
            if venID:
                ven = VEN.find_one({VEN.venID(): venID})
                if ven:
                    content = oadrCreatedPartyRegistration("452", "Invalid venID", str(requestID), str(registrationID),
                                                           None, str(VTN_ID), profiles, str(poll_freq), specific_info,
                                                           extensions)
                    return oadrPayload(content)
            ven = VEN(venID, registrationID, oadrProfileName, oadrTransportName, oadrTransportAddress,
                      oadrReportOnly, oadrXmlSignature, oadrVenName, oadrHttpPullModel)
            ven.registrationID = str(ven.venID)
        else:
            ven = VEN.find_one({VEN.registrationID():registrationID})
            if not ven or str(ven.venID) != venID:
                content = oadrCreatedPartyRegistration("452", "Invalid venID", str(requestID), str(registrationID), None, str(VTN_ID), profiles, str(poll_freq), specific_info, extensions)
                return oadrPayload(content)

            ven.oadrProfileName = oadrProfileName
            ven.oadrTransportName = oadrTransportName
            ven.oadrTransportAddress = oadrTransportAddress
            ven.oadrReportOnly = oadrReportOnly
            ven.oadrXmlSignature = oadrXmlSignature
            ven.oadrVenName = oadrVenName
            ven.oadrHttpPullModel = oadrHttpPullModel

        ven.save()
        content = oadrCreatedPartyRegistration("200", "OK", str(requestID), str(ven.registrationID), str(ven.venID), str(VTN_ID),
                                            profiles, str(poll_freq), specific_info, extensions)
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
        venID = venID_.text if venID_ is not None else None

        # respond
        ven = VEN.find_one({VEN.registrationID():registrationID})
        if str(ven.venID) != venID:
            content = oadrCanceledPartyRegistration("452", "Invalid venID", str(requestID), str(registrationID), str(venID))
            return oadrPayload(content)
        ven.remove_reports()
        ven.delete()
        content = oadrCanceledPartyRegistration("200", "OK", str(requestID), str(registrationID), str(venID))
        return oadrPayload(content)


    def _create_message(self, params):
        """ this will delete the vtn and all information without wainting for the response. We can't rely on the VEN to
        do it wrong and never be able to get rid of it"""
        registrationID = params['registrationID']
        requestID = params['requestID']
        venID = params['venID']
        ven = VEN.find_one({VEN.registrationID():registrationID})
        content = oadrCancelPartyRegistration(registrationID, requestID, venID)
        ven.remove_reports()
        ven.delete()
        return oadrPayload(content)

    def response_callback(self, response):
        response = etree.fromstring(response.text)
        if self._schema_val(response):
            final_parameters = response.xpath(".//oadr:oadrCancelPartyRegistration", namespaces=NAMESPACES)[0]
            print(final_parameters.find(".//ei:responseDescription", namespaces=NAMESPACES).text)
            #TODO: see if we have to do something with the response


class OadrCanceledPartyRegistration(OadrMessage):
    # As we have removed VEN on sending the cancel, we only need to return OK
    def _create_response(self, params):
        final_parameters = params.xpath(".//oadr:oadrCanceledPartyRegistration", namespaces=NAMESPACES)[0]
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text
        venID = final_parameters.find(".//ei:venID", namespaces=NAMESPACES).text
        content = oadrResponse("200", "OK", requestID, venID)
        return oadrPayload(content)


class OadrRequestReregistration(OadrMessage):
    def _create_message(self, params):
        # Mandatory parameters
        venID = params['venID']
        content = oadrRequestReregistration(venID)
        return oadrPayload(content)

    def response_callback(self, response):
        response = etree.fromstring(response.text)
        if self._schema_val(response):
            final_parameters = response.xpath(".//oadr:oadrResponse", namespaces=NAMESPACES)[0]
            print(final_parameters.find(".//ei:responseDescription", namespaces=NAMESPACES).text)
            #TODO: see if we have to do something with the response

"""
#Query Registration
from lxml import etree
from oadr_core.vtn.services.ei_register_party_service import OadrQueryRegistration
from oadr_core.oadr_payloads.oadr_payloads_general import pretty_print_xml
responder = OadrQueryRegistration()
xml_t = etree.parse(open('oadr_core/oadr_xml_example/ei_register_service/query_registration.xml'))
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