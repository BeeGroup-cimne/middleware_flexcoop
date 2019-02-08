from oadr_core.oadr_payloads.oadr_payloads_general import oadrPayload, pretty_print_xml, NAMESPACES
from oadr_core.oadr_payloads.oadr_payloads_register_service import oadrCreatePartyRegistration
import requests
from lxml import etree
from oadr_core.oadr_payloads.oadr_payloads_report_service import oadrRegisterReport

MIDDLEWARE_URL = "https://217.182.160.171:9022/VTN/OpenADR2/Simple/2.0b/"
MIDDLEWARE_URL = "http://127.0.0.1:8000/VTN/OpenADR2/Simple/2.0b/"


# Generate CreatePartyRegistration xml
content = oadrCreatePartyRegistration("1", "2.0b", "simpleHttp", "127.0.0.1:8080", "false", "false", "Test", "false", None, None)
# print CreatePartyRegistration xml
pretty_print_xml(content)

# make the request
r = requests.post(MIDDLEWARE_URL+"EiRegisterParty", data=etree.tostring(oadrPayload(content)), verify=False)
# see response
response = etree.fromstring(r.text)
pretty_print_xml(response)

# keep new venID

venID = response.find(".//ei:venID", namespaces=NAMESPACES).text
#venID = "363da5df-08a7-4"
# generate meta_report with all possible metadata reports
requestID = "0" # nobody requested this report
reports = [{"type": "METADATA_TELEMETRY_USAGE", "specifierID": "RP_222", "reportID": "ID_222", "duration":"P3Y6M4DT12H30M5S",
                    "datapoints":[{"id": "m3", "itembase": "current", "min_period": "P3Y6M4DT12H30M5S",
                                    "max_period": "P3Y6M4DT12H30M5S", "onChange": False, "market_context": "the market context"}]
          }]
content = oadrRegisterReport(requestID, requestID, venID, reports)

# see reports
pretty_print_xml(content)

#make the request
r = requests.post(MIDDLEWARE_URL+"EiReport", data=etree.tostring(oadrPayload(content)), verify=False)

# read report from file
with open("oadr_core/ven/report_example.xml") as f:
    rep_str = f.read()
r = requests.post(MIDDLEWARE_URL+"EiReport", data=rep_str, verify=False)
