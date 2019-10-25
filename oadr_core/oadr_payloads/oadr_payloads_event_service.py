from lxml.builder import ElementMaker

from oadr_core.oadr_payloads.oadr_payloads_general import NAMESPACES, ELEMENTS, eiResponse


def ei_event_descriptor(event):
    ei_event_descriptor = ELEMENTS['ei'].eventDescriptor(
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
        ELEMENTS['ei'].vtnComment(event.vtnComment)
    )
    return ei_event_descriptor

def ei_active_period(event):
    date_time = ELEMENTS['xcal']("date-time")
    date_time.text = event.dtstart
    dtstart = ELEMENTS['xcal'].dtstart(date_time)

    duration = ELEMENTS['xcal'].duration(
        ELEMENTS['xcal'].duration(event.duration)
    )

    if event.tolerance:
        tolerance = ELEMENTS['xcal'].torerance(
            ELEMENTS['xcal'].tolerate(
                ELEMENTS['xcal'].startafter(event.tolerance)
            )
        )
    else:
        tolerance = None

    if event.eiNotification:
        einotification = ELEMENTS['ei']("x-eiNotification")
        einotification.append(
            ELEMENTS['xcal'].duration(event.eiNotification)
        )
    else:
        einotification = None

    if event.eiRampUp:
        eirampUp = ELEMENTS['ei']("x-eiRampUp")
        eirampUp.append(
            ELEMENTS['xcal'].duration(event.eiRampUp)
        )
    else:
        eirampUp = None

    if event.eiRecovery:
        eiRecovery = ELEMENTS['ei']("x-eiRecovery")
        eiRecovery.append(
            ELEMENTS['xcal'].duration(event.eiRecovery)
        )
    else:
        eiRecovery = None

    properties = ELEMENTS['xcal'].properties()

    properties.append(dtstart)
    properties.append(duration)
    if tolerance:
        properties.append(tolerance)
    if einotification:
        properties.append(einotification)
    if eirampUp:
        properties.append(eirampUp)
    if eiRecovery:
        properties.append(eiRecovery)

    active_period = ELEMENTS['ei'].eiActivePeriod(
        properties,
        ELEMENTS['xcal'].components()
    )

    return active_period

def ei_event_signals(event):
    signals = ELEMENTS['ei'].eiEventSignals()
    for signal in event.event_signals():
        intervals_element = ELEMENTS['strm'].intervals()
        for interval in signal.signal_intervals():
            if interval.dtstart:
                dt = ELEMENTS['xcal']("date-time")
                dt.text = interval.dtstart
                dtstart = ELEMENTS['xcal'].dtstart(dt)
            else:
                dtstart = None

            if interval.duration:
                duration = ELEMENTS['xcal'].duration(
                    ELEMENTS['xcal'].duration(interval.duration)
                )
            else:
                duration = None

            if interval.uid:
                uid = ELEMENTS['xcal'].uid(
                    ELEMENTS['xcal'].text(interval.uid)
                )
            else:
                uid = None
            pay = ELEMENTS['ei'].signalPayload(
                ELEMENTS['ei'].payloadFloat(
                    ELEMENTS['ei'].value(interval.value)
                )
            )
            int = ELEMENTS['ei'].interval()
            if dtstart:
                int.append(dtstart)
            if duration:
                int.append(duration)
            if uid:
                int.append(uid)
            int.append(pay)
            intervals_element.append(int)

        if signal.target:
            eiTarget = ei_target(signal.target)
        else:
            eiTarget = None

        sig = ELEMENTS['ei'].eiEventSignal()
        sig.append(intervals_element)
        if eiTarget:
            sig.append(eiTarget)
        sig.append(ELEMENTS['ei'].signalName(signal.signalName))
        sig.append(ELEMENTS['ei'].signalType(signal.signalType))
        sig.append(ELEMENTS['ei'].signalID(signal.signalID))
        signals.append(sig)
    return signals

def ei_target(target):
    eiTarget = ELEMENTS['ei'].eiTarget(
        ELEMENTS['power'].endDeviceAsset(
            ELEMENTS['power'].mrid(target)
        )
    )
    return eiTarget

def oadrEvent(event):
    """
    Generate oadrEvent
    :return:
    """
    event_element = ELEMENTS['oadr'].oadrEvent(
        ELEMENTS['ei'].eiEvent(
            ei_event_descriptor(event),
            ei_active_period(event),
            ei_event_signals(event),
            ei_target(event.target)
        ),
        ELEMENTS['oadr'].oadrResponseRequired(event.responseRequired)
    )

    event_element.append()
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
