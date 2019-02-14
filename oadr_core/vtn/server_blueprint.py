import requests
from flask import request, abort, Response, Blueprint
from lxml import etree

import settings
from oadr_core.oadr_payloads.oadr_payloads_general import NAMESPACES
from oadr_core.vtn.models import oadrPollQueue
from oadr_core.vtn.services.ei_event_service import OadrCreatedEvent
from oadr_core.vtn.services.ei_register_party_service import OadrQueryRegistration, OadrCreatePartyRegistration, \
    OadrCancelPartyRegistration, OadrRequestReregistration
from oadr_core.vtn.services.ei_report_service import OadrRegisterReport, OadrCreatedReport, OadrUpdateReport, \
    OadrCreateReport, OadrCancelReport
from oadr_core.vtn.services.oadr_poll_service import OadrPoll

oadr = Blueprint('oadr', __name__)


OADR_MESSAGES_ENDPOINTS = {
    "EiRegisterParty": {
        'oadrQueryRegistration': OadrQueryRegistration(),
        'oadrCreatePartyRegistration': OadrCreatePartyRegistration(),
        'oadrCancelPartyRegistration': OadrCancelPartyRegistration(),
        'oadrRequestRegistration': OadrRequestReregistration()
    },
    "EiReport": {
        'oadrRegisterReport': OadrRegisterReport(),
        'oadrCreatedReport': OadrCreatedReport(),
        'oadrUpdateReport': OadrUpdateReport(),
        'oadrCreateReport': OadrCreateReport(),
        'oadrCancelReport': OadrCancelReport()
    },
    "EiEvent": {
        'oadrCreatedEvent': OadrCreatedEvent()
    },
    "OadrPoll": {
        "oadrPoll": OadrPoll()
    }
}


@oadr.route("/<service>", methods=['POST'])
def openADR_VTN_service(service):
    # read data as XML
    payload = etree.fromstring(request.get_data())
    # identify which is the message recieved:
    # TODO: Validate signed object
    root_element = payload.xpath(".//oadr:oadrSignedObject/*", namespaces=NAMESPACES)
    message = etree.QName(root_element[0].tag).localname
    responder = None
    try:
        messages = OADR_MESSAGES_ENDPOINTS[service]
        try:
            responder = messages[message]
        except KeyError as e:
            abort(Response("The message {} can't be found for this service {}".format(message, service), 400))
    except KeyError as e:
        abort(Response("The service {} can't be found".format(service), 400))
    except SyntaxError as e:
        abort(Response("Invalid schema: {}".format(e), 406))
    except NotImplementedError as e:
        abort(Response("The service {} is not implemented yet".format(message), 501))
    except Exception as e:
        abort(Response(e, 500))

    return etree.tostring(responder.respond(payload)), 200, {'Content-Type': 'text/xml; charset=utf-8'}

def send_message(oadrMessage, VEN, params):
    global oadrPollQueue
    message_payload = oadrMessage.send_oadr_message(VEN, params)
    if VEN.oadrTransportName == "simpleHttp":
        if VEN.oadrHttpPullModel:
            # if pull method, add the message to the queue for this VEN
            try:
                oadrPollQueue[VEN.venID].append(message_payload)
            except:
                oadrPollQueue[VEN.venID] = [message_payload]
            return None
        else:
            # if push method, send the message
            root_element = message_payload.xpath(".//oadr:oadrSignedObject/*", namespaces=NAMESPACES)
            message = etree.QName(root_element[0].tag).localname
            for k, v in OADR_MESSAGES_ENDPOINTS.items():
                if message in v.keys():
                    url="{}/{}".format(VEN.oadrTransportAddress, k)
                    return requests.post(url, data=etree.tostring(message_payload), verify=False)
    else:
        raise NotImplementedError("XMTP has not been implemented yet")

