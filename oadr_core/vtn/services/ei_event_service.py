from oadr_core.oadr_base_service import OadrMessage
from oadr_core.oadr_payloads.oadr_payloads_general import NAMESPACES, oadrResponse, oadrPayload
from oadr_core.vtn.configuration import VTN_ID
from oadr_core.vtn.models import VEN


class OadrCreatedEvent(OadrMessage):
    def create_response(self, params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:oadrCreatedEvent", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        eiResponse = final_parameters.find(".//ei:eiResponse", namespaces=NAMESPACES)
        venID = final_parameters.find(".//ei:venID", namespaces=NAMESPACES).text
        # Optional parameters
        eventResponses = final_parameters.find(".//ei:eventResponses")

        #respond
        if venID:
            ven = VEN.query.filter(VEN.venID == venID).first()
            if ven is None:
                content = oadrResponse("452", "Invalid venID", str(""))
                return oadrPayload(content)
        requestID = None # specifications says we should use "eventResponse:requestID" over "eiResponse:requestID"
        if eventResponses:
            responses_list = eventResponses.findall(".//ei:eventResponse")
            if len(responses_list) > 0:
                for response in responses_list:
                    # TODO: CHECK OADR EVENTS RESPONSES
                    requestID = response.find(".//pyld:requestID")
                    opt_type = response.find(".//ei:optType")
                    print("{} says {}".format(requestID, opt_type))

        if requestID is None: # we don't have any eventResponse
            requestID = eiResponse.find(".//pyld:requestID")
        content = oadrResponse("200", "OK", str(requestID), venID)
        return oadrPayload(content)


# class EventOadrPoll(OadrMessage):
#     def create_response(self, params):
#         venID = params['oadr:oadrPoll']['ei:venID']
#         return oadrDistributeEvent("200", "OK", requestID, requestID, VTN_ID, events)


#
# class OadrDistributeEvent(OadrMessage):
#     def generate_request(self):
#         #TODO: get the events and generate the payload
#         events = []
#         oadrDistributeEvent("200", "OK", requestID, requestID, VTN_ID, events)
#         pass
