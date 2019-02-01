from lxml.builder import ElementMaker

from oadr_core.oadr_payloads.oadr_payloads_general import NAMESPACES, ELEMENTS, eiResponse


def oadrEvent(eventID, modificationNumber, modificationReason, priority, marketContext, createdDateTime, eventStatus,
              testEvent, vtnComment, dtstart, duration, startafter, responseRequired):
    """
    Generate oadrEvent
    :return:
    """
    root = ElementMaker(namespace=NAMESPACES['oadr'], nsmap=NAMESPACES)
    event_element = root.oadrEvent()
    event_element.append(
        ELEMENTS['ei'].eiEvent(
            ELEMENTS['ei'].eventDescriptor(
                ELEMENTS['ei'].eventID(eventID),
                ELEMENTS['ei'].modificationNumber(modificationNumber),
                ELEMENTS['ei'].modificationReason(modificationReason),
                ELEMENTS['ei'].priority(priority),
                ELEMENTS['ei'].eiMarketContext(
                    ELEMENTS['emix'].marketContext(marketContext)
                ),
                ELEMENTS['ei'].createdDateTime(createdDateTime),
                ELEMENTS['ei'].eventStatus(eventStatus),
                ELEMENTS['ei'].testEvent(testEvent),
                ELEMENTS['ei'].vtnComment(vtnComment),
            ),
            ELEMENTS['ei'].eiActivePeriod(
                ELEMENTS['xcal'].properties(
                    ELEMENTS['xcal'].dtstart(dtstart),
                    ELEMENTS['xcal'].duration(duration),
                    ELEMENTS['xcal'].duration(startafter)
                )
            ),
            ELEMENTS['ei'].eiEventSignals(),
            ELEMENTS['ei'].eiTarget()
        )
    )
    event_element.append(ELEMENTS['oadr'].oadrResponseRequired(responseRequired))
    return event_element


def oadrDistributeEvent(response_code, response_description, response_requestId, requestID, vtnID, oadrEventsList):
    root = ElementMaker(namespace=NAMESPACES['oadr'], nsmap=NAMESPACES)
    distribute_event_element = root.oadrDistributeEvent()
    if response_code:
        distribute_event_element.append(eiResponse(response_code, response_description, response_requestId))
    distribute_event_element.append(ELEMENTS['pyld'].requestID(requestID))
    distribute_event_element.append(ELEMENTS['ei'].vtnID(vtnID))
    distribute_event_element.append(*oadrEventsList)
    return distribute_event_element
