# -*- coding: utf-8 -*-
from lxml import etree
from oadr_core.oadr_base_service import OadrMessage
from oadr_core.oadr_payloads.oadr_payloads_general import oadrPayload, NAMESPACES
from oadr_core.oadr_payloads.oadr_payloads_register_service import oadrCreatedPartyRegistration, \
    oadrCanceledPartyRegistration, oadrRequestRegistration
from oadr_core.vtn.configuration import *
from kernel.database import db_session
from oadr_core.vtn.models import VEN


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
        registrationID = registrationID_.text if registrationID_ else None
        venID_ = final_parameters.find(".//ei:venID", namespaces=NAMESPACES)
        venID = venID_.text if venID_ else None
        oadrTransportAddress_ = final_parameters.find(".//oadr:oadrTransportAddress", namespaces=NAMESPACES)
        oadrTransportAddress = oadrTransportAddress_.text if oadrTransportAddress_ else None
        oadrVenName_ = final_parameters.find(".//oadr:oadrVenName", namespaces=NAMESPACES)
        oadrVenName = oadrVenName_.text if oadrVenName_ else None
        oadrHttpPullModel_ = final_parameters.find(".//oadr:oadrHttpPullModel", namespaces=NAMESPACES)
        oadrHttpPullModel = oadrHttpPullModel_.text if oadrHttpPullModel_ else None
        oadrHttpPullModel = True if oadrHttpPullModel == 'true' else False

        # respond
        if not registrationID:
            ven = VEN(venID, registrationID, oadrProfileName, oadrTransportName, oadrTransportAddress,
                      oadrReportOnly, oadrXmlSignature, oadrVenName, oadrHttpPullModel)
            db_session.add(ven)
            db_session.flush()
            ven.registrationID = str(ven.venID)
        else:
            ven = VEN.query.filter(VEN.registrationID == registrationID).first()
            if str(ven.venID) != venID:
                db_session.rollback()
                content = oadrCreatedPartyRegistration("452", "Invalid venID", str(requestID), str(registrationID), str(ven.venID),
                                                   str(VTN_ID), profiles, str(poll_freq), specific_info, extensions)
                return oadrPayload(content)

            ven.oadrProfileName = oadrProfileName
            ven.oadrTransportName = oadrTransportName
            ven.oadrTransportAddress = oadrTransportAddress
            ven.oadrReportOnly = oadrReportOnly
            ven.oadrXmlSignature = oadrXmlSignature
            ven.oadrVenName = oadrVenName
            ven.oadrHttpPullModel = oadrHttpPullModel

        db_session.commit()
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
        venID = venID_.text if venID_ else None

        # respond
        ven = VEN.query.filter(VEN.registrationID == registrationID).first()
        if str(ven.venID) != venID:
            content = oadrCanceledPartyRegistration("452", "Invalid venID", str(requestID), str(registrationID), str(venID))
            return oadrPayload(content)

        db_session.delete(ven)
        db_session.commit()
        content = oadrCanceledPartyRegistration("200", "OK", str(requestID), str(registrationID), str(venID))
        return oadrPayload(content)


class OadrRequestRegistration(OadrMessage):
    def send(self, params):
        # Mandatory parameters
        venID = params['venID']
        content = oadrRequestRegistration(venID)
        return oadrPayload(content)

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