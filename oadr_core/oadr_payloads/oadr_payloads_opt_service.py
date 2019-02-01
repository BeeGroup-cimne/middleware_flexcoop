from oadr_core.oadr_payloads.oadr_payloads_general import ELEMENTS, eiResponse


def oadrCanceledOpt(code, description, requestID, optID):
    canceled_element = ELEMENTS['oadr'].oadrCanceledOpt(
        eiResponse(code, description, requestID),
        ELEMENTS['ei'].optID(optID)
    )
    return canceled_element

def oadrCreatedOpt(code, description, requestID, optID):
    created_element = ELEMENTS['oadr'].oadrCreatedOpt(
        eiResponse(code, description, requestID),
        ELEMENTS['ei'].optID(optID)
    )
    return created_element
