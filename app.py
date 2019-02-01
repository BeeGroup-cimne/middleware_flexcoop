# -*- coding: utf-8 -*-
import json
import settings
from flask import Flask, request, abort, Response
from lxml import etree
from lxml.builder import ElementMaker

from oadr_core.oadr_payloads.oadr_payloads_general import NAMESPACES
from oadr_core.vtn.models import VEN
from oadr_core.vtn.services.ei_event_service import OadrCreatedEvent
from oadr_core.vtn.services.ei_register_party_service import OadrQueryRegistration, OadrCreatePartyRegistration, \
    OadrCancelPartyRegistration
from oadr_core.vtn.services.ei_report_service import OadrRegisterReport, OadrCreatedReport, OadrUpdateReport, \
    OadrCreateReport, OadrCancelReport

app = Flask(__name__)

@app.route("/{prefix}/OpenADR2/Simple/2.0b/<service>".format(prefix=settings.VTN_PREFIX), methods=['POST'])
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


@app.route("/ven", methods=['GET'])
def view_ven():
    em = ElementMaker()
    response = em.div()
    response.append(em.h2("List of registered ven's"))
    list =em.ul()
    for ven in VEN.query.all():
        list.append(em.li(str(ven.venID) + " - " + str(ven.oadrVenName) +" - "+ str(ven.oadrTransportAddress)))
    response.append(list)
    return etree.tostring(response), 200

if __name__ == '__main__':
    app.run(host=settings.HOST, port=settings.PORT)

"""
import requests
from lxml import etree
import settings
xml_t = etree.parse(open('oadr_core/oadr_xml_example/ei_register_service/query_registration.xml'))
requests.post("http://{}:{}/{}/OpenADR2/Simple/2.0b/EiRegisterParty".format(settings.HOST, settings.PORT, settings.VTN_PREFIX), data=etree.tostring(xml_t), headers={"Content-Type":"text/xml"})
"""