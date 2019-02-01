from oadr_core.oadr_payloads.oadr_payloads_general import oadrPayload
from oadr_core.oadr_payloads.oadr_payloads_register_service import oadrCreatePartyRegistration
import requests
from lxml import etree
MIDDLEWARE_URL = "https://217.182.160.171:9022/VTN/OpenADR2/Simple/2.0b/"
MIDDLEWARE_URL = "http://127.0.0.1:8000/VTN/OpenADR2/Simple/2.0b/"

content = oadrCreatePartyRegistration("1", "2.0b", "simpleHttp", "127.0.0.1:8080", "false", "false", "Test", "false", None, None)

r = requests.post(MIDDLEWARE_URL+"EiRegisterParty", data=etree.tostring(oadrPayload(content)), verify=False)
