# -*- coding: utf-8 -*-
from flask_pymongo import PyMongo

import settings
from flask import Flask
from oadr_core.vtn.server_blueprint import oadr
from visual_interface.visual_blueprint import web

app = Flask(__name__)
app.config.from_object('settings')
mongo = PyMongo(app)

#openADR blueprint
app.register_blueprint(oadr, url_prefix="/{prefix}/OpenADR2/Simple/2.0b".format(prefix=settings.VTN_PREFIX))
app.register_blueprint(web, url_prefix="/web")


if __name__ == '__main__':
    app.run(host=settings.HOST, port=settings.PORT)

"""
import requests
from lxml import etree
import settings
xml_t = etree.parse(open('oadr_core/oadr_xml_example/ei_register_service/query_registration.xml'))
requests.post("http://{}:{}/{}/OpenADR2/Simple/2.0b/EiRegisterParty".format(settings.HOST, settings.PORT, settings.VTN_PREFIX), data=etree.tostring(xml_t), headers={"Content-Type":"text/xml"})
"""