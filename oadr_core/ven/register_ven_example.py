from datetime import datetime

from oadr_core.oadr_payloads.oadr_payloads_general import oadrPayload, pretty_print_xml, NAMESPACES
from oadr_core.oadr_payloads.oadr_payloads_poll_service import oadrPoll
from oadr_core.oadr_payloads.oadr_payloads_register_service import oadrCreatePartyRegistration, \
    oadrCancelPartyRegistration
import requests
from lxml import etree
from oadr_core.oadr_payloads.oadr_payloads_report_service import oadrRegisterReport

MIDDLEWARE_URL = "http://217.182.160.171:9022/VTN/OpenADR2/Simple/2.0b/"
MIDDLEWARE_URL = "http://127.0.0.1:8000/VTN/OpenADR2/Simple/2.0b/"

xmlparser = etree.XMLParser()
schema_val_data = etree.parse(open("oadr_core/oadr_xml_schema/oadr_20b.xsd"), xmlparser)
schema_val = etree.XMLSchema(schema_val_data)

venID = "511dc82a-ea7d-11e9-a8e9-ac1f6b403fbc"
registrationID = ""

def register_ven(venID = None, registrationID= None):
    # Generate CreatePartyRegistration xml
    content = oadrCreatePartyRegistration("1", "2.0b", "simpleHttp", "127.0.0.1:8080", False, False, "Test", True, registrationID,
                                          venID)
    r = requests.post(MIDDLEWARE_URL+"EiRegisterParty", data=etree.tostring(oadrPayload(content)), verify=False)
    response = etree.fromstring(r.text)
    code = response.find(".//ei:responseCode", namespaces=NAMESPACES).text
    description = response.find(".//ei:responseDescription", namespaces=NAMESPACES).text
    if code != "200":
        raise Exception("error invalid code {} {}".format(code, description))
    venID = response.find(".//ei:venID", namespaces=NAMESPACES).text
    registrationID = response.find(".//ei:registrationID", namespaces=NAMESPACES).text

    return venID, registrationID

def send_metadata_report(venID):
    # requestID = "0"  # nobody requested this report
    # reports = [{"reportName": "METADATA_TELEMETRY_USAGE", "reportSpecifierID": "RP_223", "eiReportID": "ID_222",
    #             "duration": "P3Y6M4DT12H30M5S",
    #             "reportRequestID": "0", "createdDateTime": datetime.utcnow().isoformat(),
    #             "data_points": [{"rID": "m3", "reportType": "usage", "readingType": "Direct Read",
    #                              "oadrMinPeriod": "P3Y6M4DT12H30M5S",
    #                              "oadrMaxPeriod": "P3Y6M4DT12H30M5S", "oadrOnChange": False,
    #                              "market_context": "the market context"}]
    #             }]
    # content = oadrRegisterReport(requestID, requestID, venID, reports)
    content = etree.parse("oadr_core/oadr_xml_example/new_example.xml", xmlparser)
    r = requests.post(MIDDLEWARE_URL + "EiReport", data=etree.tostring(content), verify=False)
    response = etree.fromstring(r.text)
    schema_val(response)
    pretty_print_xml(response)

def send_test_report(venID):
    # requestID = "0"  # nobody requested this report
    # reports = [{"reportName": "METADATA_TELEMETRY_USAGE", "reportSpecifierID": "RP_223", "eiReportID": "ID_222",
    #             "duration": "P3Y6M4DT12H30M5S",
    #             "reportRequestID": "0", "createdDateTime": datetime.utcnow().isoformat(),
    #             "data_points": [{"rID": "m3", "reportType": "usage", "readingType": "Direct Read",
    #                              "oadrMinPeriod": "P3Y6M4DT12H30M5S",
    #                              "oadrMaxPeriod": "P3Y6M4DT12H30M5S", "oadrOnChange": False,
    #                              "market_context": "the market context"}]
    #             }]
    # content = oadrRegisterReport(requestID, requestID, venID, reports)
    content = etree.parse("oadr_core/oadr_xml_example/ei_report_service/test_status.xml", xmlparser)
    r = requests.post(MIDDLEWARE_URL + "EiReport", data=etree.tostring(content), verify=False)
    response = etree.fromstring(r.text)
    schema_val(response)
    pretty_print_xml(response)


venID, registrationID = register_ven(venID, venID)

send_metadata_report(venID)
send_test_report(venID)


#     content = oadrPoll(venID)
#     r = requests.post(MIDDLEWARE_URL + "OadrPoll", data=etree.tostring(oadrPayload(content)), verify=False)
#     response = etree.fromstring(r.text)
#     schema_val(response)
#     pretty_print_xml(response)
#
#
#
#
#
#
#
#
#
# #registrationID = response.find(".//ei:registrationID", namespaces=NAMESPACES).text
#
# #cancel registration
#
# #content = oadrCancelPartyRegistration(registrationID, "0", venID)
# #r = requests.post(MIDDLEWARE_URL+"EiRegisterParty", data=etree.tostring(oadrPayload(content)), verify=False)
#
# #venID = "363da5df-08a7-4"
# # generate meta_report with all possible metadata reports
# requestID = "0" # nobody requested this report
# reports = [{"reportName": "METADATA_TELEMETRY_USAGE", "reportSpecifierID": "RP_222", "eiReportID": "ID_222", "duration":"P3Y6M4DT12H30M5S",
#             "reportRequestID":"0", "createdDateTime": datetime.now().isoformat(),
#                     "data_points":[{"rID": "m3", "reportType": "usage", "readingType": "Direct Read", "oadrMinPeriod": "P3Y6M4DT12H30M5S",
#                                     "oadrMaxPeriod": "P3Y6M4DT12H30M5S", "oadrOnChange": False, "market_context": "the market context"}]
#           }]
# content = oadrRegisterReport(requestID, requestID, venID, reports)
#
# # see reports
# pretty_print_xml(content)
#
# #make the request
# r = requests.post(MIDDLEWARE_URL+"EiReport", data=etree.tostring(oadrPayload(content)), verify=False)
# response = etree.fromstring(r.text)
# schema_val(response)
# pretty_print_xml(response)
# #generate oadrPoll method
#
# content = oadrPoll(venID)
#
# pretty_print_xml(content)
# r = requests.post(MIDDLEWARE_URL+"OadrPoll", data=etree.tostring(oadrPayload(content)), verify=False)
# response = etree.fromstring(r.text)
# schema_val(response)
# pretty_print_xml(response)
# # read report from file
# with open("oadr_core/ven/report_example.xml") as f:
#     rep_str = f.read()
# r = requests.post(MIDDLEWARE_URL+"EiReport", data=rep_str, verify=False)
