import time

import requests
from flask import request, abort, Response, Blueprint, current_app as app
from lxml import etree

import settings
from oadr_core.oadr_payloads.oadr_payloads_general import NAMESPACES
from oadr_core.vtn.after_request import AfterResponse
from oadr_core.vtn.models import oadrPollQueue, VEN
from oadr_core.vtn.services.ei_event_service import OadrCreatedEvent, OadrDistributeEvent
from oadr_core.vtn.services.ei_register_party_service import OadrQueryRegistration, OadrCreatePartyRegistration, \
    OadrCancelPartyRegistration, OadrRequestReregistration, OadrCanceledPartyRegistration
from oadr_core.vtn.services.ei_report_service import OadrRegisterReport, OadrCreatedReport, OadrUpdateReport, \
    OadrCreateReport, OadrCancelReport, OadrRegisteredReport
from oadr_core.vtn.services.oadr_poll_service import OadrPoll
from blinker import Namespace


my_signals = Namespace()
ven_registered = my_signals.signal('ven-registered')
registered_report_reports = my_signals.signal('registered_report_with_reports')
oadr = Blueprint('oadr', __name__)


OADR_MESSAGES_ENDPOINTS = {
    "EiRegisterParty": {
        'oadrQueryRegistration': (OadrQueryRegistration(),{}),
        'oadrCreatePartyRegistration': (OadrCreatePartyRegistration(), {'recieve':[ven_registered]}),
        'oadrCancelPartyRegistration': (OadrCancelPartyRegistration(),{}),
        'oadrCanceledPartyRegistration': (OadrCanceledPartyRegistration(),{}),
        'oadrRequestRegistration': (OadrRequestReregistration(),{})
    },
    "EiReport": {
        'oadrRegisterReport': (OadrRegisterReport(),{'send_push':[registered_report_reports]}),
        'oadrRegisteredReport':(OadrRegisteredReport(), {'recieve':[registered_report_reports]}),
        'oadrCreatedReport': (OadrCreatedReport(),{}),
        'oadrUpdateReport': (OadrUpdateReport(),{}),
        'oadrCreateReport': (OadrCreateReport(),{}),
        'oadrCancelReport': (OadrCancelReport(),{})
    },
    "EiEvent": {
        'oadrCreatedEvent': (OadrCreatedEvent(), {}),
        'oadrDistributeEvent': (OadrDistributeEvent(), {})
    },
    "OadrPoll": {
        "oadrPoll": (OadrPoll(), {})
    }
}

@oadr.route("/<service>", methods=['POST'])
def openADR_VTN_service(service):
    # read data as XML
    payload = etree.fromstring(request.get_data())
    # identify which is the message recieved:
    # TODO: Validate signed object
    print(request.headers)

    root_element = payload.xpath(".//oadr:oadrSignedObject/*", namespaces=NAMESPACES)
    message = etree.QName(root_element[0].tag).localname
    responder = None
    try:
        messages = OADR_MESSAGES_ENDPOINTS[service]
        try:
            responder, events = messages[message]
            response = etree.tostring(responder.respond(payload))
            if 'recieve' in events:
                app.response_callback.append((events['recieve'], response))

            return response, 200, {'Content-Type': 'text/xml; charset=utf-8'}
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


def send_message(oadrMessage, VEN, params):
    global oadrPollQueue
    message_payload = oadrMessage.send_oadr_message(VEN, params)

    if VEN.oadrTransportName == "simpleHttp":
        if VEN.oadrHttpPullModel:
            # if pull method, add the message to the queue for this VEN
            try:
                oadrPollQueue[VEN.venID].append((oadrMessage.__class__.__name__,  message_payload))
            except:
                oadrPollQueue[VEN.venID] = [(oadrMessage.__class__.__name__, message_payload)]
            return None
        else:
            # if push method, send the message
            root_element = message_payload.xpath(".//oadr:oadrSignedObject/*", namespaces=NAMESPACES)
            message = etree.QName(root_element[0].tag).localname
            for k, v in OADR_MESSAGES_ENDPOINTS.items():
                if message in v.keys():
                    events = v[message][1]['send_push'] if 'send_push' in v[message][1] else []
                    url="{}/{}".format(VEN.oadrTransportAddress, k)
                    response = requests.post(url, data=etree.tostring(message_payload), verify=False)
                    if response and response.ok:
                        oadrMessage.response_callback(response)
                        for event in events:
                            event.send(oadr, response=response.text)
                    return response
    else:
        raise NotImplementedError("XMTP has not been implemented yet")



@ven_registered.connect_via(AfterResponse)
def when_VEN_registered(sender, response, **extra):
    payload = etree.fromstring(response)
    venID = payload.find(".//ei:venID", namespaces=NAMESPACES).text
    ven = VEN.find_one({VEN.venID():venID})
    registerReport = OadrRegisterReport()
    send_message(registerReport, ven, {})

@registered_report_reports.connect_via(oadr)
def when_report_in_registered_report(sender, response, **extra):
    payload = etree.fromstring(response)
    venID = payload.find(".//ei:venID", namespaces=NAMESPACES).text
    ven = VEN.find_one({VEN.venID(): venID})
    createdReport = OadrCreatedReport()
    send_message(createdReport, ven, {})