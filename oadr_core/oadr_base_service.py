# -*- coding: utf-8 -*-
from lxml import etree


class OadrMessage():
    _schema_val = None
    def __init__(self):
        if not OadrMessage._schema_val:
            xmlparser = etree.XMLParser()
            schema_val_data = etree.parse(open("oadr_core/oadr_xml_schema/oadr_20b.xsd"), xmlparser)
            OadrMessage._schema_val = etree.XMLSchema(schema_val_data)

    def respond(self, params):
        if self._schema_val(params):
            print("schema_valid")
            return self._create_response(params)
        else:
            print("schema_invalid")
            log = self._schema_val.error_log
            print(log.last_error)
            raise SyntaxError(log.last_error)

    def _create_response(self, params):
        raise NotImplementedError("The response should be created at service subclass")

    def send_oadr_message(self, VEN, params):
        params['venID'] = VEN.venID
        message_payload = self._create_message(params)
        return message_payload

    def _create_message(self, params):
        raise NotImplementedError("The message should be created at service subclass")

    def response_callback(self, response):
        pass

def createVENID():
    return 0

def createRegistrationId():
    return 0
