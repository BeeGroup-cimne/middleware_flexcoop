from lxml.builder import ElementMaker

from oadr_core.oadr_payloads.oadr_payloads_general import NAMESPACES, ELEMENTS, eiResponse


def ei_event_descriptor(event):
    ei_event_descriptor = ELEMENTS['ei'].eventDescriptor()
    ei_event_descriptor.append(ELEMENTS['ei'].eventID(event.event_id))
    ei_event_descriptor.append(ELEMENTS['ei'].modificationNumber(event.modification_number))
    if event.modification_date_time:
        ei_event_descriptor.append(event.modification_date_time.strftime("%Y-%m-%dT%H:%M:%S"))
    if event.modification_reason:
        ei_event_descriptor.append(ELEMENTS['ei'].modificationReason(event.modification_reason))
    if event.priority:
        ei_event_descriptor.append(ELEMENTS['ei'].priority(event.priority))
    ei_event_descriptor.append(
        ELEMENTS['ei'].eiMarketContext(
            ELEMENTS['emix'].marketContext(event.market_context)
        )
    )

    ei_event_descriptor.append(ELEMENTS['ei'].createdDateTime(event.created_date_time.strftime("%Y-%m-%dT%H:%M:%S")))
    ei_event_descriptor.append(ELEMENTS['ei'].eventStatus(event.event_status))
    if event.test_event:
        ei_event_descriptor.append(ELEMENTS['ei'].testEvent('true' if event.test_event else 'false'))
    if event.vtn_comment:
        ei_event_descriptor.append(ELEMENTS['ei'].vtnComment(event.vtn_comment))

    return ei_event_descriptor

def ei_active_period(event):
    date_time = ELEMENTS['xcal']("date-time")
    date_time.text = event.dtstart.strftime("%Y-%m-%dT%H:%M:%S" if event.dtstart else '')
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

    if event.ei_notification:
        einotification = ELEMENTS['ei']("x-eiNotification")
        einotification.append(
            ELEMENTS['xcal'].duration(event.ei_notification)
        )
    else:
        einotification = None

    if event.ei_ramp_up:
        eirampUp = ELEMENTS['ei']("x-eiRampUp")
        eirampUp.append(
            ELEMENTS['xcal'].duration(event.ei_ramp_up)
        )
    else:
        eirampUp = None

    if event.ei_recovery:
        eiRecovery = ELEMENTS['ei']("x-eiRecovery")
        eiRecovery.append(
            ELEMENTS['xcal'].duration(event.ei_recovery)
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
                dt.text = interval.dtstart.strftime("%Y-%m-%dT%H:%M:%S" if interval.dtstart else '')
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
        sig.append(ELEMENTS['ei'].signalName(signal.signal_name))
        sig.append(ELEMENTS['ei'].signalType(signal.signal_type))
        sig.append(ELEMENTS['ei'].signalID(signal.signal_id))
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
        ELEMENTS['oadr'].oadrResponseRequired(event.response_required)
    )
    return event_element


def oadrDistributeEvent(response_code, response_description, response_requestId, requestID, vtnID, events):
    root = ElementMaker(namespace=NAMESPACES['oadr'], nsmap=NAMESPACES)
    distribute_event_element = root.oadrDistributeEvent()
    if response_code:
        distribute_event_element.append(eiResponse(response_code, response_description, response_requestId))
    distribute_event_element.append(ELEMENTS['pyld'].requestID(requestID))
    distribute_event_element.append(ELEMENTS['ei'].vtnID(vtnID))
    for event in events:
        oadrEvents = oadrEvent(event)
        distribute_event_element.append(oadrEvents)
    return distribute_event_element
