from lxml.builder import ElementMaker

from oadr_core.oadr_payloads.oadr_payloads_general import NAMESPACES, ELEMENTS, eiResponse
from project_customization.flexcoop.models import EventSignal, EventInterval


def oadrEvent(event):
    """
    Generate oadrEvent
    :return:
    """
    event_element = ELEMENTS['oadr'].oadrEvent()

    dt = ELEMENTS['xcal']("date-time")
    dt.text = event.dtstart
    einotification = ELEMENTS['ei']("x-eiNotification")
    einotification.append(
        ELEMENTS['xcal'].duration(event.eiNotification)
    )
    eirampUp = ELEMENTS['ei']("x-eiRampUp")
    eirampUp.append(
        ELEMENTS['xcal'].duration(event.eiRampUp)
    )
    eiRecovery = ELEMENTS['ei']("x-eiRecovery")
    eiRecovery.append(
        ELEMENTS['xcal'].duration(event.eiRecovery)
    )
    ei_event = ELEMENTS['ei'].eiEvent(
        ELEMENTS['ei'].eventDescriptor(
            ELEMENTS['ei'].eventID(event.eventID),
            ELEMENTS['ei'].modificationNumber(event.modificationNumber),
            ELEMENTS['ei'].modificationDateTime(event.modificationDateTime),
            ELEMENTS['ei'].modificationReason(event.modificationReason),
            ELEMENTS['ei'].priority(event.priority),
            ELEMENTS['ei'].eiMarketContext(
                ELEMENTS['emix'].marketContext(event.marketContext)
            ),
            ELEMENTS['ei'].createdDateTime(event.createdDateTime),
            ELEMENTS['ei'].eventStatus(event.eventStatus),
            ELEMENTS['ei'].testEvent(event.testEvent),
            ELEMENTS['ei'].vtnComment(event.vtnComment),
        ),
        ELEMENTS['ei'].eiActivePeriod(
            ELEMENTS['xcal'].properties(
                ELEMENTS['xcal'].dtstart(event.dtstart),
                ELEMENTS['xcal'].duration(
                    ELEMENTS['xcal'].duration(event.duration)
                ),
                ELEMENTS['xcal'].torerance(
                    ELEMENTS['xcal'].tolerate(event.tolerance)
                ),
                einotification,
                eirampUp,
                eiRecovery
            ),
            ELEMENTS['xcal'].components()
        )
    )
    signals = ELEMENTS['ei'].eiEventSignals()
    for signal in EventSignal.find({EventSignal.event(): event._id}):
        intervals_element = ELEMENTS['strm'].intervals()
        dt = ELEMENTS['xcal']("date-time")
        dt.text=interval.dtstart
        for interval in EventInterval.find({EventInterval.signal: signal._id}):
            intervals_element.append(
                ELEMENTS['ei'].interval(
                    ELEMENTS['xcal'].dtstart(dt),
                    ELEMENTS['xcal'].duration(
                        ELEMENTS['xcal'].duration(interval.duration)
                    ),
                    ELEMENTS['xcal'].uid(
                        ELEMENTS['xcal'].text(interval.uid)
                    )
                )
            )
        signals.append(
            ELEMENTS['ei'].eiEventSignal(
                intervals_element,
                ELEMENTS['ei'].signalName(signal.signalName),
                ELEMENTS['ei'].signalType(signal.signalType),
                ELEMENTS['ei'].signalID(signal.signalID)
            )
        )
    #TODO get target of event and add it.
    # eiTarget = ELEMENTS['ei'].eiTarget()
    ei_event.append(signals)

    event_element.append(ei_event)
    event_element.append(ELEMENTS['oadr'].oadrResponseRequired(event.responseRequired))
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
