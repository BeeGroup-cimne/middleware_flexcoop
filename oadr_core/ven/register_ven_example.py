from oadr_core.oadr_payloads.oadr_payloads_general import oadrPayload, pretty_print_xml, NAMESPACES
from oadr_core.oadr_payloads.oadr_payloads_poll_service import oadrPoll
from oadr_core.oadr_payloads.oadr_payloads_register_service import oadrCreatePartyRegistration
import requests
from lxml import etree
from oadr_core.oadr_payloads.oadr_payloads_report_service import oadrRegisterReport

MIDDLEWARE_URL = "http://217.182.160.171:9022/VTN/OpenADR2/Simple/2.0b/"
MIDDLEWARE_URL = "http://127.0.0.1:8000/VTN/OpenADR2/Simple/2.0b/"

xmlparser = etree.XMLParser()
schema_val_data = etree.parse(open("oadr_core/oadr_xml_schema/oadr_20b.xsd"), xmlparser)
schema_val = etree.XMLSchema(schema_val_data)

# Generate CreatePartyRegistration xml
content = oadrCreatePartyRegistration("1", "2.0b", "simpleHttp", "127.0.0.1:8080", False, False, "Test", True, None, None)
# print CreatePartyRegistration xml
pretty_print_xml(content)

# make the request
r = requests.post(MIDDLEWARE_URL+"EiRegisterParty", data=etree.tostring(oadrPayload(content)), verify=False)
# see response
response = etree.fromstring(r.text)
if not schema_val(response):
    log = schema_val.error_log
    print(log.last_error)
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
response = etree.fromstring(r.text)
if not schema_val(response):
    log = schema_val.error_log
    print(log.last_error)
pretty_print_xml(response)

#generate oadrPoll method
content = oadrPoll(venID)

pretty_print_xml(content)
r = requests.post(MIDDLEWARE_URL+"OadrPoll", data=etree.tostring(oadrPayload(content)), verify=False)
response = etree.fromstring(r.text)
if not schema_val(response):
    log = schema_val.error_log
    print(log.last_error)
pretty_print_xml(response)
# read report from file
with open("oadr_core/ven/report_example.xml") as f:
    rep_str = f.read()
r = requests.post(MIDDLEWARE_URL+"EiReport", data=rep_str, verify=False)
