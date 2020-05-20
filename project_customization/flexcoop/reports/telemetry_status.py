import pandas as pd
import re
from builtins import hasattr
from datetime import datetime, timedelta

import requests
from flask import request, current_app as app

from mongo_orm import MongoDB, AnyField
from oadr_core.exceptions import InvalidReportException
from oadr_core.oadr_payloads.oadr_payloads_general import ELEMENTS, NAMESPACES
from oadr_core.oadr_payloads.reports.report import OadrReport
from project_customization.flexcoop.models import map_rid_device_id, Device
from project_customization.flexcoop.utils import parse_rid, status_mapping, get_id_from_rid, convert_snake_case, \
    get_middleware_token
import threading
import logging

def hypertech_send(data):
    hypertech_url = "https://adsl.hypertech.gr:444/flexcoop/services/middlewareData"
    hypertech_cert = False
    #hypertech_direct_send:
    logging.basicConfig(level=logging.DEBUG)
    log = logging.getLogger("Hypertech")
    with requests.Session() as s:
        for d in data:
            try:
                hypertech_json = {
                    "rId": d['rid'],
                    "value": d['value'],
                    "timestamp": d['dt']
                }
                token = get_middleware_token()
                headers = {'Authorization': token}
                s.post(hypertech_url, headers=headers, json=hypertech_json, verify=hypertech_cert)
            except Exception as e:
                log.debug("***********", e)

def get_data_model(element):
    class ReportDataModel(MongoDB):
        "A telemetry usage report data"
        __collectionname__ = element
        report_id = AnyField()
        dtstart = AnyField()
        duration = AnyField()
        uid = AnyField()
        confidence = AnyField()
        accuracy = AnyField()
        data_quality = AnyField()
        value = AnyField()
        device_id = AnyField()
        account_id = AnyField()
        aggregator_id = AnyField()

        def __init__(self, deviceID, report_id, dtstart, duration, uid, confidence, accuracy, dataQuality, value):
            self.device_id = deviceID
            self.report_id = report_id
            self.dtstart = dtstart
            self.duration = duration
            self.uid = uid
            self.confidence = confidence
            self.accuracy = accuracy
            self.data_quality = dataQuality
            self.value = value
            try:
                self.account_id = request.cert['CN'] if hasattr(request, "cert") and 'CN' in request.cert else None
                self.aggregator_id = request.cert['O'] if hasattr(request, "cert") and 'O' in request.cert else None
            except:
                self.account_id = None
                self.aggregator_id = None
    return ReportDataModel

def get_report_models():
    class TelemetryStatusReportModel(MongoDB):
        "A telemetry status report"
        __collectionname__ = "telemetry_status"
        dtstart = AnyField()
        duration = AnyField()
        report_id = AnyField()
        report_request_id = AnyField()
        specifier_id = AnyField()
        report_name = AnyField()
        created_date_time = AnyField()

        def __init__(self, dt_start, duration, reportID, reportRequestID, specifierID, createdDateTime):
            self.dtstart = dt_start
            self.duration = duration
            self.report_id = reportID
            self.report_request_id = reportRequestID
            self.specifier_id = specifierID
            self.report_name = TelemetryStatusReport.report_name
            self.created_date_time = createdDateTime

    return TelemetryStatusReportModel

class TelemetryStatusReport(OadrReport):
    report_name = "TELEMETRY_STATUS"

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
            ELEMENTS['ei'].reportName("TELEMETRY_STATUS"),
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
        report = get_report_models()
        dt_start = oadrReport.find(".//xcal:dtstart", namespaces=NAMESPACES)
        duration = oadrReport.find(".//xcal:duration", namespaces=NAMESPACES)
        reportID_p = oadrReport.find(".//ei:eiReportID", namespaces=NAMESPACES).text
        reportRequestID_p = oadrReport.find(".//ei:reportRequestID", namespaces=NAMESPACES).text
        specifierID = oadrReport.find(".//ei:reportSpecifierID", namespaces=NAMESPACES).text
        createdDateTime = oadrReport.find(".//ei:createdDateTime", namespaces=NAMESPACES).text
        duration_p = duration.find(".//xcal:date-time", namespaces=NAMESPACES).text if duration.find(".//xcal:date-time", namespaces=NAMESPACES) is not None else ""
        dt_start_p = dt_start.find(".//xcal:date-time", namespaces=NAMESPACES).text if dt_start.find(".//xcal:date-time", namespaces=NAMESPACES) is not None else ""
        r = report(dt_start_p, duration_p, reportID_p, reportRequestID_p, specifierID, createdDateTime)
        print("report_created")
        r.save()
        report_id = r._id
        intervals = oadrReport.find(".//strm:intervals", namespaces=NAMESPACES)
        hypertech_data = []
        mongo_data = {}
        exception = None
        errors = []
        try:
            account_id = request.cert['CN'] if hasattr(request, "cert") and 'CN' in request.cert else None
            aggregator_id = request.cert['O'] if hasattr(request, "cert") and 'O' in request.cert else None
        except:
            account_id = None
            aggregator_id = None

        for interval in intervals.findall(".//ei:interval", namespaces=NAMESPACES):
            # We will only update the status to the Device endpoint
            dt_start = interval.find(".//xcal:dtstart", namespaces=NAMESPACES)
            duration = interval.find(".//xcal:duration", namespaces=NAMESPACES)
            uid = interval.find(".//xcal:uid", namespaces=NAMESPACES)
            rid_i = interval.find(".//ei:rID", namespaces=NAMESPACES).text

            confidence = interval.find(".//ei:confidence", namespaces=NAMESPACES)
            accuracy = interval.find(".//ei:accuracy", namespaces=NAMESPACES)
            dataQuality = interval.find(".//ei:dataQuality", namespaces=NAMESPACES)
            value_i = interval.find(".//oadr:oadrCurrent", namespaces=NAMESPACES).text

            duration_i = duration.find(".//xcal:date-time", namespaces=NAMESPACES).text if duration.find(
                ".//xcal:date-time", namespaces=NAMESPACES) is not None else ""
            dt_start_i = dt_start.find(".//xcal:date-time", namespaces=NAMESPACES).text if dt_start.find(
                ".//xcal:date-time", namespaces=NAMESPACES) is not None else ""
            uid_i = uid.find(".//xcal:text", namespaces=NAMESPACES).text if dt_start.find(
                ".//xcal:text", namespaces=NAMESPACES) is not None else ""
            confidence_i = confidence.text if confidence is not None else ""
            accuracy_i = accuracy.text if accuracy is not None else ""
            dataQuality_i = dataQuality.text if dataQuality is not None else ""

            hypertech_data.append({"rid": rid_i, "value": value_i, "dt": dt_start_i})

            phisical_device, pdn, groupID, spaces, load, ln, metric = parse_rid(rid_i)
            if metric not in status_mapping.keys():
                continue

            json = {
                "report_id": report_id,#
                "dtstart": dt_start_i,#
                "duration": duration_i,#
                "uid": uid_i,#
                "confidence": confidence_i,#
                "accuracy": accuracy_i,#
                "data_quality": dataQuality_i,#
                "value": value_i,#
                "device_id": get_id_from_rid(rid_i),#
                "account_id": account_id,#
                "aggregator_id": aggregator_id,#
                "_updated_at": datetime.utcnow(),#
                "_created_at": datetime.utcnow()#
            }

            try:
                mongo_data[convert_snake_case("{}_{}".format("status", status_mapping[metric]))].append(json)
            except:
                mongo_data[convert_snake_case("{}_{}".format("status", status_mapping[metric]))] = [json]

        send_thread = threading.Thread(target=hypertech_send, args=(hypertech_data,))
        send_thread.start()

        for metric, data in mongo_data.items():
            df = pd.DataFrame.from_records(data)
            id_mappings = {}
            device_mappings = {}
            rids = df.device_id.unique()
            for rid in rids:
                mapping = map_rid_device_id.find_one({map_rid_device_id.rid(): rid})
                if mapping:
                    id_mappings[rid] = mapping.device_id
                    device_mappings[mapping.device_id] = Device.find_one({Device.device_id(): mapping.device_id})
                else:
                    errors.append(rid)
                    id_mappings[rid] = None
                    device_mappings[rid] = None

            def get_anonimized_id(rid):
                try:
                    return id_mappings[rid]
                except:
                    return None

            df.device_id = df.device_id.apply(get_anonimized_id)

            # calculate if some recent changes for the device
            now = datetime.utcnow() - timedelta(minutes=1)
            df2 = df[pd.to_datetime(df.dtstart) > now]
            if not df2.empty:
                grouped = df2.groupby("device_id")
                for device_id, g in grouped:
                    max_value = g.loc[g.dtstart == g.dtstart.max()]
                    device = device_mappings[device_id]
                    if device:
                        cvalue = max_value.value.values[0]
                        device.status[status_mapping[metric]].update({"value": cvalue})
                        device.save()
            # save all historics
            TMP = get_data_model(metric)
            upload_data = df.to_dict(orient="records")
            TMP.__mongo__.insert_many(upload_data)
