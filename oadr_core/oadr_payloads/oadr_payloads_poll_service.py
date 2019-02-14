from oadr_core.oadr_payloads.oadr_payloads_general import ELEMENTS


def oadrPoll(venID):
    oadr_poll_element = ELEMENTS['oadr'].oadrPoll(
        ELEMENTS['ei'].venID(venID)
    )
    return oadr_poll_element