from oadr_core.oadr_base_service import OadrMessage
from oadr_core.oadr_payloads.oadr_payloads_general import NAMESPACES, oadrPayload
from oadr_core.oadr_payloads.oadr_payloads_opt_service import oadrCreatedOpt, oadrCanceledOpt


class OadrCreateOpt(OadrMessage):
    def create_response(self, params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:adrCreateOpt", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        optID = final_parameters.find(".//ei:optID", namespaces=NAMESPACES).text
        optType = final_parameters.find(".//ei:optType", namespaces=NAMESPACES).text
        optReason = final_parameters.find(".//ei:optReason", namespaces=NAMESPACES).text
        venID = final_parameters.find(".//ei:venID", namespaces=NAMESPACES).text
        created_time = final_parameters.find(".//ei:createdDateTime", namespaces=NAMESPACES).text
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text
        eiTarget = final_parameters.find(".//ei:eiTarget", namespaces=NAMESPACES)


        # Optional parameters
        market_context_ = final_parameters.find(".//emix:marketContext", namespaces=NAMESPACES)
        market_context = market_context_.text if market_context_ else None
        qualifiedEventID = final_parameters.find(".//ei:qualifiedEventID", namespaces=NAMESPACES)
        vavailability = final_parameters.find(".//xcal:vavailability", namespaces=NAMESPACES)
        oadrDeviceClass = final_parameters.find(".//oadr:oadrDeviceClass", namespaces=NAMESPACES)

        # respond
        # TODO: Create opt
        content = oadrCreatedOpt("200", "OK", str(requestID), str(optID))
        return oadrPayload(content)


class OadrCancelOpt(OadrMessage):
    def create_response(self, params):
        # get information of needed parameters
        final_parameters = params.xpath(".//oadr:adrCreateOpt", namespaces=NAMESPACES)[0]

        # Mandatory parameters
        requestID = final_parameters.find(".//pyld:requestID", namespaces=NAMESPACES).text
        venID = final_parameters.find(".//ei:venID", namespaces=NAMESPACES).text
        optID = final_parameters.find(".//ei:optID", namespaces=NAMESPACES).text

        # Optional parameters
        # """""

        # TODO: Cancel opt
        content = oadrCanceledOpt("200", "OK", str(requestID))
        return oadrPayload(content)
