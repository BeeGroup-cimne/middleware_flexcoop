from oadr_core.oadr_base_service import OadrMessage
from project_customization.flexcoop.models import oadrPollQueue
from oadr_core.oadr_payloads.oadr_payloads_general import NAMESPACES, oadrResponse, oadrPayload, pretty_print_xml


class OadrPoll(OadrMessage):
    def _create_response(self, params):
        final_parameters = params.xpath(".//oadr:oadrPoll", namespaces=NAMESPACES)[0]
        venID = final_parameters.find(".//ei:venID", namespaces=NAMESPACES).text
        if venID in oadrPollQueue and oadrPollQueue[venID]:
            type, response = oadrPollQueue[venID].pop(0)
        else:
            response = oadrPayload(oadrResponse("200", "OK", "0", venID))
        return response