from flask import request, abort, Response, Blueprint
from lxml import etree

import settings
from oadr_core.oadr_payloads.oadr_payloads_general import NAMESPACES
from oadr_core.vtn.services.ei_event_service import OadrCreatedEvent
from oadr_core.vtn.services.ei_register_party_service import OadrQueryRegistration, OadrCreatePartyRegistration, \
    OadrCancelPartyRegistration
from oadr_core.vtn.services.ei_report_service import OadrRegisterReport, OadrCreatedReport, OadrUpdateReport, \
    OadrCreateReport, OadrCancelReport

oadr =  Blueprint('oadr', __name__)

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
        if service == "EiRegisterParty":
            if message == 'oadrQueryRegistration':
                responder = OadrQueryRegistration()
            elif message == 'oadrCreatePartyRegistration':
                responder = OadrCreatePartyRegistration()
            elif message == 'oadrCancelPartyRegistration':
                responder = OadrCancelPartyRegistration()
            else:
                abort(Response("The message {} can't be found for this service {}".format(message, service), 400))
        elif service == "EiReport":
            if message == 'oadrRegisterReport':
                responder = OadrRegisterReport()
            elif message == 'oadrCreatedReport':
                responder = OadrCreatedReport()
            elif message == 'oadrUpdateReport':
                responder = OadrUpdateReport()
            elif message == 'oadrCreateReport':
                responder = OadrCreateReport()
            elif message == 'oadrCancelReport':
                responder = OadrCancelReport()
            else:
                abort(Response("The message {} can't be found for this service {}".format(message, service), 400))
        elif service == 'EiEvent':
            if message == 'oadrCreatedEvent':
                responder = OadrCreatedEvent()
            else:
                abort(Response("The message {} can't be found for this service {}".format(message, service), 400))
        elif service == 'EiOpt':
            raise NotImplementedError()
        elif service == 'OadrPoll':
            raise NotImplementedError()
        else:
            abort(Response("The service {} can't be found".format(service), 400))
    except SyntaxError as e:
        abort(Response("Invalid schema: {}".format(e), 406))
    except NotImplementedError as e:
        abort(Response("The service {} is not implemented yet".format(message),501))
    except Exception as e:
        abort(Response(e,500))

    return etree.tostring(responder.respond(payload)), 200, {'Content-Type': 'text/xml; charset=utf-8'}
