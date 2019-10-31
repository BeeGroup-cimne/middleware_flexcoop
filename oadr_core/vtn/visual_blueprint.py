from datetime import datetime
from flask import Blueprint, render_template, request, redirect, url_for

from oadr_core.vtn.server_blueprint import send_message
from oadr_core.vtn.services.ei_event_service import OadrDistributeEvent
from project_customization.flexcoop.models import VEN, MetadataReports, oadrPollQueue, DataPoint, Event, EventSignal, \
    EventInterval, Device, map_rid_device_id
from oadr_core.vtn.services.ei_register_party_service import OadrCancelPartyRegistration, OadrRequestReregistration
from oadr_core.vtn.services.ei_report_service import OadrCreateReport, OadrCancelReport
from oadr_core.vtn.web_forms import EventForm, EventSignalForm
from project_customization.flexcoop.utils import convert_camel_case

web = Blueprint('visual', __name__, template_folder='templates')

@web.route("/ven", methods=['GET'])
def view_ven_list():
    return render_template("web/ven/ven_list.html", ven_list=VEN.find({}))

@web.route("/ven/<venID>", methods=['GET'])
def view_ven_info(venID):
    ven = VEN.find_one({VEN.ven_id(): venID})
    try:
        poll_messages= oadrPollQueue[venID]
    except:
        poll_messages=[]
    return render_template("web/ven/ven.html", ven=ven, poll_messages=poll_messages)


@web.route("/ven/<venID>/delete", methods=['GET'])
def view_ven_delete(venID):
    ven = VEN.find_one({VEN.ven_id(): venID})
    cancel_registration = OadrCancelPartyRegistration()
    params = {
        "registrationID": ven.registration_id,
        "requestID": "0"
    }
    response = send_message(cancel_registration, ven, params)
    return redirect(url_for("visual.view_ven_list"))

@web.route("/ven/<venID>/reregister", methods=['GET'])
def view_ven_reregister(venID):
    ven = VEN.find_one({VEN.ven_id(): venID})
    re_registration = OadrRequestReregistration()
    params = {
    }
    response = send_message(re_registration, ven, params)
    return redirect(url_for("visual.view_ven_list"))

@web.route("/ven_reports/<venID>", methods=['GET','POST'])
def view_ven_reports(venID):
    ven = VEN.find_one({VEN.ven_id(): venID})
    reports = MetadataReports.find({MetadataReports.ven(): ven._id})
    report_data_points = {}
    for report in reports:
        report_data_points[report] = { "data_points" : DataPoint.find({DataPoint.report(): report._id}),
                                       "devices" : Device.find({Device.report(): report._id})}

    if request.method == "POST":
        register_reports = []
        cancel_reports = []
        for report, data_points in report_data_points.items():
            register_data_points = []
            non_registered_data_points = []
            change = False
            for data_point in data_points['data_points']:
                for k, v in data_point.reporting_items.items():
                    subscription = True if request.form.get("r{}-{}-{}".format(report.ei_report_id, data_point.rid, k)) else False
                    if subscription:
                        if not v['subscribed']:
                            change = True
                            register_data_points.append((data_point, v['oadr_name'], v['reading_type']))
                    else:
                        if v['subscribed']:
                            change = True
                            non_registered_data_points.append(data_point)
                    data_point.reporting_items[k]['subscribed'] = subscription
                    data_point.save()

            for device in data_points['devices']:
                for k, v in device.status.items():
                    subscription = True if request.form.get("r{}-{}-{}".format(report.ei_report_id, device.rid, k)) else False
                    if subscription:
                        if not v['subscribed']:
                            change = True
                            register_data_points.append((device, v['oadr_name'], v['reading_type']))
                    else:
                        if v['subscribed']:
                            change=True
                            non_registered_data_points.append(device)
                    device.status[k]['subscribed'] = subscription
                    device.save()

            if change:
                if register_data_points:
                    report.subscribed = True
                    register_reports.append((report, register_data_points))
                else:
                    report.subscribed = False
                    cancel_reports.append(report)
                report.save()
        # TODO: Set requestID properly
        if register_reports:
            createReport = OadrCreateReport()
            report_types = [{"reportId":x.ei_report_id,
                             "specifierId": x.specifier_id,
                             "data_points":[
                                 {
                                     'rid': "{}_{}".format(map_rid_device_id.find_one({map_rid_device_id.device_id(): d[0].device_id}).rid, d[1]),
                                     'reading_type': d[2]
                                 } for d in y]
                            } for x, y in register_reports]
            params = {
                "requestID": "0",
                "report_types": report_types
            }
            response = send_message(createReport, ven, params)
            # TODO do something with the response if required by the protocol. When oadrPoll response will be None

        if cancel_reports:
             cancelReport = OadrCancelReport()
             params = {
                 "requestID": "0",
                 "cancelReport": [x.ei_report_id for x in cancel_reports],
                 "followUp": False,
             }
             response = send_message(cancelReport, ven, params)
             # TODO do something with the response if required by the protocol. When oadrPoll response will be None
    return render_template("web/ven/ven_reports.html", ven=ven, report_data_points=report_data_points)

@web.route("/vtn_reports/",  methods=['GET'])
def view_vtn_reports():
    reports = MetadataReports.find({MetadataReports.owned():True})
    return render_template("web/vtn_reports/vtn_reports.html", reports=reports)

@web.route("/vtn_create_reports/",  methods=['GET', 'POST'])
def create_vtn_reports():
    return render_template("web/vtn_reports/vtn_create_report.html")

@web.route("/vtn_events/",  methods=['GET'])
def view_vtn_events():
    events = Event.find({})
    return render_template("web/events/list_events.html", events=events)

@web.route("/vtn_create_events/",  methods=['GET', 'POST'])
def create_vtn_events():
    form=EventForm()
    if request.method=="POST":
        form = EventForm(request.form)
        print(request.form)
        if form.validate():
            data = request.form
            description_dt_start = datetime.strptime(data['description-dtstart'], "%Y-%m-%d %H:%M:%S") if data['description-dtstart'] else None
            interval_dt_start = datetime.strptime(data['interval-dtstart'], "%Y-%m-%d %H:%M:%S") if data['interval-dtstart'] else None
            description_testEvent = True if 'description-testEvent' in data and data['description-testEvent']=="y" else False

            event = Event(data['description-priority'], data['description-marketContext'], data['description-eventStatus'],
                          description_testEvent, data['description-vtnComment'], description_dt_start,
                          data['description-duration'], data['description-tolerance'], data['description-eiNotification'],
                          data['description-eiRampUp'], data['description-eiRecovery'], data['description-target'],  data['description-responseRequired'])
            print(event)
            event.save()
            signal = EventSignal(event._id, data['signal-target'], data['signal-signalID'], data['signal-signalType'], data['signal-signalName'], data['signal-currentValue'])
            signal.save()
            interval = EventInterval(signal._id, interval_dt_start, data['interval-duration'], data['interval-uid'], data['interval-value'])
            interval.save()
            send_message(OadrDistributeEvent(), VEN.find_one({VEN.ven_id():data['ven']}), {'event_list':[event], "requestID": "1"})
            return redirect(url_for("visual.view_vtn_events"))
        else:
            print("A")
    return render_template("web/events/create_events.html", form=form)

