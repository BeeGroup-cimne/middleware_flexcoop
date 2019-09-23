from mongo_orm import MongoDB, AnyField
from oadr_core.oadr_payloads.oadr_payloads_general import ELEMENTS, NAMESPACES
from oadr_core.oadr_payloads.reports.report import OadrReport



class TelemetryUsageReport(OadrReport):
    report_name = "TELEMETRY_USAGE"

    def get_report_models(self):
        class TelemetryReportModel(MongoDB):
            "A telemetry report"
            __collectionname__ = "reports"
            dtstart = AnyField()
            duration = AnyField()
            reportID = AnyField()
            reportRequestID = AnyField()
            specifierID = AnyField()
            reportName = AnyField()
            createdDateTime = AnyField()
            dataTypes = AnyField()
            def __init__(self, dt_start, duration, reportID, reportRequestID, specifierID, createdDateTime):
                self.dtstart = dt_start
                self.duration = duration
                self.reportID = reportID
                self.reportRequestID = reportRequestID
                self.specifierID = specifierID
                self.reportName = TelemetryUsageReport.report_name
                self.createdDateTime = createdDateTime
                self.dataType = set()

            def add_dataType(self, datatype):
                self.dataType.add(datatype)
                self.save()
            def remove_dataType(self, datatype):
                self.dataType.remove(datatype)
                self.save()
        return TelemetryReportModel

    def get_datapoint_model(self, rid_i):
        class ReportDataModel(MongoDB):
            "A telemetry report datapoint"
            __collectionname__ = rid_i
            report_id = AnyField()
            dtstart = AnyField()
            duration = AnyField()
            uid = AnyField()
            rid = AnyField()
            confidence = AnyField()
            accuracy = AnyField()
            dataQuality = AnyField()
            value = AnyField()

            def __init__(self, report_id, dt_start, duration, uid, rid, confidence, accuracy, dataQuality, value):
                self.report_id = report_id
                self.dtstart = dt_start
                self.duration = duration
                self.uid = uid
                self.rid = rid
                self.confidence = confidence
                self.accuracy = accuracy
                self.dataQuality = dataQuality
                self.value = value

        return ReportDataModel

    def create(self, reportRequestId, reportSpecifierID, created, reportID, dt_start, duration, intervals):
        """
        Generates a real telemetry usage report
        :param reportRequestId:  the request id
        :param reportSpecifierID: the report specifier id
        :param created: the datetime of created in iso format
        :param reportID: the id of the report
        :param dt_start: the start of the report
        :param duration: the duration of the report
        :param intervals: a list of dictionaries indicating each interval of time:
                {dtstart: the start of the interval,
                duration: the duration of the interval,
                uid: an id of this interval starting at 0,
                datapooints: a list of dictionaris of the datapoints for this report:
                    {rid: the identifier of the datapoint,
                    payload: the actual data of this datapoint,
                    confidence: the confidence value,
                    accuracy: the accuracy of the measure,
                    dataQuality: description on the quality of this data,
                    }
        :return:
        """
        oadr_report_element = ELEMENTS['oadr'].oadrReport(
            ELEMENTS['ei'].eiReportID(reportID),
            ELEMENTS['ei'].reportRequestID(reportRequestId),
            ELEMENTS['ei'].reportSpecifierID(reportSpecifierID),
            ELEMENTS['ei'].createdDateTime(created),
            ELEMENTS['ei'].reportName("TELEMETRY_USAGE"),
        )
        if dt_start:
            dt = ELEMENTS['xcal']("date-time")
            dt.text = dt_start
            oadr_report_element.append(
                ELEMENTS['xcal'].dtstart(
                    dt
                )
            )
        if duration:
            dt = ELEMENTS['xcal']("date-time")
            dt.text = dt_start
            oadr_report_element.append(
                ELEMENTS['xcal'].duration(
                    ELEMENTS['xcal'].duration(
                        duration
                    )
                )
            )
        if intervals:
            intervals_element = ELEMENTS['strm'].intervals()
            for interval in intervals:
                dt = ELEMENTS['xcal']("date-time")
                dt.text = interval['dtstart']
                interval_element = ELEMENTS['ei'].interval(
                    ELEMENTS['xcal'].dtstart(dt),
                    ELEMENTS['xcal'].duration(
                        ELEMENTS['xcal'].duration(
                            interval['duration']
                        )
                    ),
                    ELEMENTS['xcal'].uid(
                        ELEMENTS['xcal'].text(interval['uid'])
                    ),
                )
                if 'datapoints' in interval:
                    for datapoint in interval['datapoints']:
                        report_payload = ELEMENTS['oadr'].oadrReportPayload(
                            ELEMENTS['ei'].rid(datapoint['rid']),
                            ELEMENTS['ei'].payloadFloat(
                                ELEMENTS['ei'].value(datapoint['payload'])
                            )
                        )
                        if 'confidence' in datapoint:
                            report_payload.append(
                                ELEMENTS['ei'].confidence(datapoint['confidence'])
                            )
                        if 'accuracy' in datapoint:
                            report_payload.append(
                                ELEMENTS['ei'].accuracy(datapoint['accuracy'])
                            )
                        if 'dataQuality' in datapoint:
                            report_payload.append(
                                ELEMENTS['oadr'].oadrDataQuality(datapoint['dataQuality'])
                            )
                        interval_element.append(report_payload)
                intervals_element.append(interval_element)
            oadr_report_element.append(intervals_element)
        return oadr_report_element


    def parse(self, oadrReport):
        report = self.get_report_models()
        dt_start = oadrReport.find(".//xcal:dtstart", namespaces=NAMESPACES)
        duration = oadrReport.find(".//xcal:duration", namespaces=NAMESPACES)
        reportID_p = oadrReport.find(".//ei:eiReportID", namespaces=NAMESPACES).text
        reportRequestID_p = oadrReport.find(".//ei:reportRequestID", namespaces=NAMESPACES).text
        specifierID = oadrReport.find(".//ei:reportSpecifierID", namespaces=NAMESPACES).text
        createdDateTime = oadrReport.find(".//ei:createdDateTime", namespaces=NAMESPACES).text
        duration_p = duration.find(".//xcal:date-time", namespaces=NAMESPACES).text if duration.find(".//xcal:date-time", namespaces=NAMESPACES) is not None else ""
        dt_start_p = dt_start.find(".//xcal:date-time", namespaces=NAMESPACES).text if dt_start.find(".//xcal:date-time", namespaces=NAMESPACES) is not None else ""
        r = report(dt_start_p, duration_p, reportID_p, reportRequestID_p, specifierID, createdDateTime)
        r.save()
        report_id = r._id
        intervals = oadrReport.find(".//strm:intervals", namespaces=NAMESPACES)
        for interval in intervals.findall(".//ei:interval", namespaces=NAMESPACES):
            dt_start = interval.find(".//xcal:dtstart", namespaces=NAMESPACES)
            duration = interval.find(".//xcal:duration", namespaces=NAMESPACES)
            uid = interval.find(".//xcal:uid", namespaces=NAMESPACES)
            rid_i = interval.find(".//ei:rID", namespaces=NAMESPACES).text
            confidence = interval.find(".//ei:confidence", namespaces=NAMESPACES)
            accuracy = interval.find(".//ei:accuracy", namespaces=NAMESPACES)
            dataQuality = interval.find(".//ei:dataQuality", namespaces=NAMESPACES)
            value_i = interval.find(".//ei:value", namespaces=NAMESPACES).text

            duration_i = duration.find(".//xcal:date-time", namespaces=NAMESPACES).text if duration.find(
                ".//xcal:date-time", namespaces=NAMESPACES) is not None else ""
            dt_start_i = dt_start.find(".//xcal:date-time", namespaces=NAMESPACES).text if dt_start.find(
                ".//xcal:date-time", namespaces=NAMESPACES) is not None else ""
            uid_i = uid.find(".//xcal:text", namespaces=NAMESPACES).text if dt_start.find(
                ".//xcal:text", namespaces=NAMESPACES) is not None else ""
            confidence_i = confidence.text if confidence is not None else ""
            accuracy_i = accuracy.text if accuracy is not None else ""
            dataQuality_i = dataQuality.text if dataQuality is not None else ""

            TMP = self.get_datapoint_model(rid_i)

            data = TMP(report_id, dt_start_i, duration_i, uid_i, rid_i, confidence_i, accuracy_i, dataQuality_i, value_i)
            data.save()
            r.add_dataType(rid_i)
            r.save()