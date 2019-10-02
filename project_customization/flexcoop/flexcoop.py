from oadr_core.exceptions import InvalidVenException
from project_customization.flexcoop.models import VEN

class FlexcoopCustomization():
    profiles = {'2.0b': ['simpleHttp', 'xmpp'], '2.0a': ['simpleHttp', 'xmpp']}
    VTN_ID = 1
    poll_freq = "P3Y6M4DT12H30M5S"
    specific_info = {}  # {"EiEvent":["haha",10]}
    extensions = {}  # {"extension1":["haha",10]}
    reports_to_subscribe = ["Ligth", "HVAC", "DHW"]

    def on_OadrCreatePartyRegistration_recieve(self, requestID, oadrProfileName, oadrTransportName, oadrReportOnly, oadrXmlSignature,
                             registrationID, venID, oadrTransportAddress, oadrVenName, oadrHttpPullModel):

        # Check for correct ven and registrationID
        if not registrationID:
            if venID:
                ven = VEN.find_one({VEN.venID(): venID})
                if ven:
                    raise InvalidVenException()

            ven = VEN(venID, registrationID, oadrProfileName, oadrTransportName, oadrTransportAddress,
                      oadrReportOnly, oadrXmlSignature, oadrVenName, oadrHttpPullModel)
            ven.registrationID = str(ven.venID)
        else:
            ven = VEN.find_one({VEN.registrationID():registrationID})
            if not ven or str(ven.venID) != venID:
                raise InvalidVenException()

        # save info of new ven
        ven.oadrProfileName = oadrProfileName
        ven.oadrTransportName = oadrTransportName
        ven.oadrTransportAddress = oadrTransportAddress
        ven.oadrReportOnly = oadrReportOnly
        ven.oadrXmlSignature = oadrXmlSignature
        ven.oadrVenName = oadrVenName
        ven.oadrHttpPullModel = oadrHttpPullModel
        ven.save()

        return "200", "OK", ven.registrationID, ven.venID

    def on_OadrCancelPartyRegistration_recieve(self, requestID, registrationID, venID):

        ven = VEN.find_one({VEN.registrationID(): registrationID})
        if str(ven.venID) != venID:
            raise InvalidVenException()
        ven.remove_reports()
        ven.delete()
        return "200", "OK"

    def on_OadrCancelPartyRegistration_send(self, registrationID, requestID, venID):
        ven = VEN.find_one({VEN.registrationID(): registrationID})
        ven.remove_reports()
        ven.delete()

    def on_OadrCancelPartyRegistration_response(self):
        # TODO: see if we have to do something with the response
        print("missing_parameters TODO")

    def on_OadrCanceledPartyRegistration_recieve(self, requestID, venID):
        # As we have removed VEN on sending the cancel, we only need to return OK
        return "200", "OK"

    def on_OadrRequestReregistration_send(self, venID):
        pass

    def on_OadrRequestReregistration_response(self):
        # TODO: see if we have to do something with the response
        print("missing_parameters TODO")