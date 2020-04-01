import requests
from OpenSSL import crypto
from flask import request, abort, Response, Blueprint, current_app as app
from flask_pymongo import MongoClient
from lxml import etree

from project_customization.flexcoop.models import oadrPollQueue
from oadr_core.oadr_payloads.oadr_payloads_general import NAMESPACES, pretty_print_xml
from oadr_core.vtn.after_request import AfterResponse
from project_customization.flexcoop.models import VEN
from oadr_core.vtn.services.ei_event_service import OadrCreatedEvent, OadrDistributeEvent
from oadr_core.vtn.services.ei_register_party_service import OadrQueryRegistration, OadrCreatePartyRegistration, \
    OadrCancelPartyRegistration, OadrRequestReregistration, OadrCanceledPartyRegistration
from oadr_core.vtn.services.ei_report_service import OadrRegisterReport, OadrCreatedReport, OadrUpdateReport, \
    OadrCreateReport, OadrCancelReport, OadrRegisteredReport
from oadr_core.vtn.services.oadr_poll_service import OadrPoll
from blinker import Namespace

from settings import MONGO_URI

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
    app.logger.debug(request.headers)
    cert_string = request.headers['X-Ssl-Cert'].replace("&", "\n").replace("\t","")
    app.logger.debug("request revieved to {}".format(service))
    app.logger.debug(repr(cert_string))
    cert_obj = crypto.load_certificate(crypto.FILETYPE_PEM, cert_string)
    if cert_obj.has_expired():
        abort(Response("Invalid certificate"), 403)
    request.cert = {k.decode("utf-8"):v.decode("utf-8") for k,v in cert_obj.get_subject().get_components()}
    app.logger.debug("accepted cert")
    # TODO: Validate signed object

    #access_log = MongoClient(MONGO_URI).get_database()['access_log']
    #access_log.insert_one({"IP": str(request.environ.get('HTTP_X_REAL_IP', request.remote_addr)), "USER": str(request.cert['CN'])})

    root_element = payload.xpath(".//oadr:oadrSignedObject/*", namespaces=NAMESPACES)
    message = etree.QName(root_element[0].tag).localname
    app.logger.debug(message)
    responder = None
    try:
        messages = OADR_MESSAGES_ENDPOINTS[service]
        try:
            responder, events = messages[message]
            app.logger.debug("going to respond the request")
            response = etree.tostring(responder.respond(payload))
            if 'recieve' in events:
                app.response_callback.append((events['recieve'], response))
            if message == "oadrUpdateReport":
                try:
                    value = payload.find(".//ei:rID", namespaces=NAMESPACES).text
                    print(value)
                except:
                    print("notvalid rid query")

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
        app.logger.info(e)
        abort(Response(e, 500))


def send_message(oadrMessage, VEN, params):
    global oadrPollQueue
    message_payload = oadrMessage.send_oadr_message(VEN, params)

    if VEN.oadr_transport_name == "simpleHttp":
        if VEN.oadr_http_pull_model:
            # if pull method, add the message to the queue for this VEN
            try:
                oadrPollQueue[VEN.ven_id].append((oadrMessage.__class__.__name__,  message_payload))
            except:
                oadrPollQueue[VEN.ven_id] = [(oadrMessage.__class__.__name__, message_payload)]
            return None
        else:
            # if push method, send the message
            root_element = message_payload.xpath(".//oadr:oadrSignedObject/*", namespaces=NAMESPACES)
            message = etree.QName(root_element[0].tag).localname
            for k, v in OADR_MESSAGES_ENDPOINTS.items():
                if message in v.keys():
                    events = v[message][1]['send_push'] if 'send_push' in v[message][1] else []
                    url="{}/{}".format(VEN.oadr_transport_address, k)
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
    ven_id = payload.find(".//ei:venID", namespaces=NAMESPACES).text
    ven = VEN.find_one({VEN.ven_id():ven_id})
    registerReport = OadrRegisterReport()
    send_message(registerReport, ven, {})

@registered_report_reports.connect_via(oadr)
def when_report_in_registered_report(sender, response, **extra):
    payload = etree.fromstring(response)
    ven_id = payload.find(".//ei:venID", namespaces=NAMESPACES).text
    ven = VEN.find_one({VEN.ven_id(): ven_id})
    createdReport = OadrCreatedReport()
    send_message(createdReport, ven, {})