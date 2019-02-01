# -*- coding: utf-8 -*-
from lxml import etree
import xmltodict

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
            raise SyntaxError(log.last_error)

    def _create_response(self, params):
        raise NotImplementedError("The response should be created at service subclass")

    def send(self, params):
        raise NotImplementedError("The message should be created at service subclass")


def createVENID():
    return 0

def createRegistrationId():
    return 0
