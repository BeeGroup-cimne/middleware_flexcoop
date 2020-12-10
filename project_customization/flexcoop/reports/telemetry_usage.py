import pandas as pd
import re
from datetime import datetime

import requests
from pymongo import MongoClient

import settings
from mongo_orm import MongoDB, AnyField
from oadr_core.exceptions import InvalidReportException
from oadr_core.oadr_payloads.oadr_payloads_general import ELEMENTS, NAMESPACES
from oadr_core.oadr_payloads.reports.report import OadrReport
from project_customization.flexcoop.models import map_rid_device_id
from project_customization.flexcoop.utils import parse_rid, get_id_from_rid, convert_snake_case, get_middleware_token
from flask import request, current_app as app
import threading
import logging
from project_customization.flexcoop.timeseries_utils import timeseries_mapping



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

def get_report_models():
    class TelemetryUsageReportModel(MongoDB):
        "A telemetry report"
        __collectionname__ = "telemetry_usage"
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
            self.report_name = TelemetryUsageReport.report_name
            self.created_date_time = createdDateTime

    return TelemetryUsageReportModel

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
        device_class = AnyField()

        def __init__(self, deviceID, report_id, dtstart, duration, uid, confidence, accuracy, dataQuality, value, device_class):
            self.device_id = deviceID
            self.report_id = report_id
            self.dtstart = dtstart
            self.duration = duration
            self.uid = uid
            self.confidence = confidence
            self.accuracy = accuracy
            self.data_quality = dataQuality
            self.value = value
            self.device_class = device_class
            try:
                self.account_id = request.cert['CN'] if hasattr(request, "cert") and 'CN' in request.cert else None
                self.aggregator_id = request.cert['O'] if hasattr(request, "cert") and 'O' in request.cert else None
            except:
                self.account_id = None
                self.aggregator_id = None
    return ReportDataModel


class TelemetryUsageReport(OadrReport):
    report_name = "TELEMETRY_USAGE"

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
        conn = MongoClient(settings.MONGO_URI)
        database = conn.get_database("flexcoop")
        report = get_report_models()
        dtstart = oadrReport.find(".//xcal:dtstart", namespaces=NAMESPACES)
        duration = oadrReport.find(".//xcal:duration", namespaces=NAMESPACES)
        report_id_p = oadrReport.find(".//ei:eiReportID", namespaces=NAMESPACES).text
        report_request_id_p = oadrReport.find(".//ei:reportRequestID", namespaces=NAMESPACES).text
        specifier_id = oadrReport.find(".//ei:reportSpecifierID", namespaces=NAMESPACES).text
        created_date_time = oadrReport.find(".//ei:createdDateTime", namespaces=NAMESPACES).text
        duration_p = duration.find(".//xcal:date-time", namespaces=NAMESPACES).text if duration.find(".//xcal:date-time", namespaces=NAMESPACES) is not None else ""
        dtstart_p = dtstart.find(".//xcal:date-time", namespaces=NAMESPACES).text if dtstart.find(".//xcal:date-time", namespaces=NAMESPACES) is not None else ""
        r = report(dtstart_p, duration_p, report_id_p, report_request_id_p, specifier_id, created_date_time)
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
            dtstart = interval.find(".//xcal:dtstart", namespaces=NAMESPACES)
            duration = interval.find(".//xcal:duration", namespaces=NAMESPACES)
            uid = interval.find(".//xcal:uid", namespaces=NAMESPACES)
            rid_i = interval.find(".//ei:rID", namespaces=NAMESPACES).text

            confidence = interval.find(".//ei:confidence", namespaces=NAMESPACES)
            accuracy = interval.find(".//ei:accuracy", namespaces=NAMESPACES)
            data_quality = interval.find(".//ei:dataQuality", namespaces=NAMESPACES)
            value_i = interval.find(".//ei:value", namespaces=NAMESPACES).text

            duration_i = duration.find(".//xcal:date-time", namespaces=NAMESPACES).text if duration.find(
                ".//xcal:date-time", namespaces=NAMESPACES) is not None else ""
            dtstart_i = dtstart.find(".//xcal:date-time", namespaces=NAMESPACES).text if dtstart.find(
                ".//xcal:date-time", namespaces=NAMESPACES) is not None else ""
            uid_i = uid.find(".//xcal:text", namespaces=NAMESPACES).text if dtstart.find(
                ".//xcal:text", namespaces=NAMESPACES) is not None else ""
            confidence_i = confidence.text if confidence is not None else ""
            accuracy_i = accuracy.text if accuracy is not None else ""
            data_quality_i = data_quality.text if data_quality is not None else ""


            hypertech_data.append({"rid": rid_i, "value": value_i, "dt": dtstart_i})

            phisical_device, pdn, groupID, spaces, load, ln, metric = parse_rid(rid_i)
            metric = convert_snake_case(metric)
            if metric not in timeseries_mapping.keys():
                continue
            json = {
                "report_id": report_id,  #
                "dtstart": dtstart_i,  #
                "duration": duration_i,  #
                "uid": uid_i,  #
                "confidence": confidence_i,  #
                "accuracy": accuracy_i,  #
                "data_quality": data_quality_i,  #
                "value": value_i,  #
                "device_id": get_id_from_rid(rid_i),  #
                "account_id": account_id,  #
                "aggregator_id": aggregator_id,  #
                "device_class": load,#
                "_updated_at": datetime.utcnow(),  #
                "_created_at": datetime.utcnow()  #
            }

            try:
                mongo_data[metric].append(json)
            except:
                mongo_data[metric] = [json]

        send_thread = threading.Thread(target=hypertech_send, args=(hypertech_data,))
        send_thread.start()

        for metric, data in mongo_data.items():
            df = pd.DataFrame.from_records(data)
            id_mappings = {}
            rids = df.device_id.unique()
            for rid in rids:
                mapping = map_rid_device_id.find_one({map_rid_device_id.rid(): rid})
                if mapping:
                    id_mappings[rid] = mapping.device_id
                else:
                    errors.append(rid)
                    id_mappings[rid] = None

            def get_anonimized_id(rid):
                try:
                    return id_mappings[rid]
                except:
                    return None

            df.device_id = df.device_id.apply(get_anonimized_id)

            df = df.dropna(subset=['device_id'])
            device_id_df = df.device_id.unique()[0]
            data_point = database['data_points'].find({"device_id": device_id_df})[0]

            if not data_point:
                continue
            if not data_point['reporting_items'][metric]['subscribed']:
                continue
            #save all historics
            TMP = get_data_model(metric)
            upload_data = df.to_dict(orient="records")
            TMP.__mongo__.insert_many(upload_data)
