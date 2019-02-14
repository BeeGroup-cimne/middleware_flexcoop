# -*- coding: utf-8 -*-
from lxml.builder import ElementMaker
from oadr_core.oadr_payloads.oadr_payloads_general import ELEMENTS, eiResponse


# sub_documents for register_service
def oadrProfiles(profiles):
    """
    Generates the eiProfiles document required by the oadrCreatedPartyRegistration
    :param profiles: a dictionary in the form { <profile_name>:[<allowed transports>]}
    :return: the oadrProfiles document
    """
    transports_elements = {n: ELEMENTS['oadr'].oadrTransports(
        *[ELEMENTS['oadr'].oadrTransport(ELEMENTS['oadr'].oadrTransportName(t_name))
          for t_name in v]) for n, v in profiles.items()}

    profiles_elements = [ELEMENTS['oadr'].oadrProfile(ELEMENTS['oadr'].oadrProfileName(name), transports_elements[name])
                         for name in profiles.keys()]
    oadr_profile_element = ELEMENTS['oadr'].oadrProfiles(*profiles_elements)

    return oadr_profile_element


def oadrServiceSpecificInfo(specific_info):
    """
    Generates the specific info for oadr services indicating the key and value
    :param specific_info: a dictionary in the form {<service>:[key,value]}
    :return: the oadrServiceSpecificInfo document
    """
    services_elements = [
        ELEMENTS['oadr'].oadrService(
            ELEMENTS['oadr'].oadrServiceName(service),
            ELEMENTS['oadr'].oadrInfo(
                ELEMENTS['oadr'].oadrKey(str(v[0])),
                ELEMENTS['oadr'].oadrValue(str(v[1]))
            )
        )
        for service, v in specific_info.items()
    ]
    return ELEMENTS['oadr'].oadrServiceSpecificInfo(*services_elements)


def oadrExtensions(extension_info):
    """
    Generates the oadrExtensions indicating hte key and value
    :param extension_info: a dictionary in the form {<extension>:[key,value]}
    :return: the oadrExtensions document
    """
    extension_elements = [
        ELEMENTS['oadr'].oadrExtension(
            ELEMENTS['oadr'].oadrExtensionName(extension),
            ELEMENTS['oadr'].oadrInfo(
                ELEMENTS['oadr'].oadrKey(str(v[0])),
                ELEMENTS['oadr'].oadrValue(str(v[1]))
            )
        )
        for extension, v in extension_info.items()
    ]
    return ELEMENTS['oadr'].oadrExtensions(*extension_elements)


# OADR registration payloads
def oadrCreatedPartyRegistration(response_code, response_description, response_requestId, registrationID, venID, vtnID,
                                 profiles, poll_freq, specific_info, extensions):
    """
    Generates the oadrCreatedPartiRegistration with the vtnInfo
    :param response_code:
    :param response_description:
    :param response_requestId:
    :param registrationID:
    :param venID:
    :param vtnID:
    :param profiles:
    :param poll_freq:
    :param specific_info:
    :param extensions:
    :return: the oadrCreatedPartyRegistration document
    """

    party_registration_element = ELEMENTS['oadr'].oadrCreatedPartyRegistration()
    party_registration_element.append(eiResponse(response_code, response_description, response_requestId))
    if registrationID:
        party_registration_element.append(ELEMENTS['ei'].registrationID(registrationID))
    if venID:
        party_registration_element.append(ELEMENTS['ei'].venID(venID))
    party_registration_element.append(ELEMENTS['ei'].vtnID(vtnID))
    party_registration_element.append(oadrProfiles(profiles))
    party_registration_element.append(ELEMENTS['oadr'].oadrRequestedOadrPollFreq(ELEMENTS['xcal'].duration(poll_freq)))
    if specific_info:
        party_registration_element.append(oadrServiceSpecificInfo(specific_info))
    if extensions:
        party_registration_element.append(oadrExtensions(extensions))

    return party_registration_element


def oadrCanceledPartyRegistration(response_code, response_description, response_requestId, registrationID, venID):
    """
    Generates the oadrCancelledPartyRegistration
    :param response_code:
    :param response_description:
    :param response_requestId:
    :param registrationID:
    :param venID:
    :return:
    """
    cancel_registration_element = ELEMENTS['oadr'].oadrCanceledPartyRegistration()
    cancel_registration_element.append(eiResponse(response_code, response_description, response_requestId))
    cancel_registration_element.append(ELEMENTS['ei'].registrationID(registrationID))
    cancel_registration_element.append(ELEMENTS['ei'].venID(venID))

    return cancel_registration_element

def oadrRequestReregistration(venID):
    """
    Generates the oadrRequestReregistration
    :param venID:
    :return:
    """
    request_registration_element = ELEMENTS['oadr'].oadrRequestReregistration(
        ELEMENTS['ei'].venID(venID)
    )
    return request_registration_element

def oadrCreatePartyRegistration(requestID, profileName, transportName, transportAddress, reportOnly, signature,
                                venName, pull, registrationID, venID):
    create_registration_element = ELEMENTS['oadr'].oadrCreatePartyRegistration(
        ELEMENTS['pyld'].requestID(requestID)
    )
    if registrationID:
        create_registration_element.append(ELEMENTS['ei'].registrationID(registrationID))

    if venID:
        create_registration_element.append(ELEMENTS['ei'].venID(venID))
    create_registration_element.append(ELEMENTS['oadr'].oadrProfileName(profileName))
    create_registration_element.append(ELEMENTS['oadr'].oadrTransportName(transportName))
    create_registration_element.append(ELEMENTS['oadr'].oadrTransportAddress(transportAddress))
    create_registration_element.append(ELEMENTS['oadr'].oadrReportOnly('true' if reportOnly else 'false'))
    create_registration_element.append(ELEMENTS['oadr'].oadrXmlSignature('true' if signature else 'false'))
    create_registration_element.append(ELEMENTS['oadr'].oadrVenName(venName))
    create_registration_element.append(ELEMENTS['oadr'].oadrHttpPullModel('true' if pull else 'false'))

    return create_registration_element
