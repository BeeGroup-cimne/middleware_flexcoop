from flask import Blueprint, render_template, request, redirect, url_for

from oadr_core.vtn.server_blueprint import send_message
from project_customization.flexcoop.models import VEN, MetadataReports, oadrPollQueue, DataPoint, Event, EventSignal, \
    EventInterval, Device
from oadr_core.vtn.services.ei_register_party_service import OadrCancelPartyRegistration, OadrRequestReregistration
from oadr_core.vtn.services.ei_report_service import OadrCreateReport, OadrCancelReport
from oadr_core.vtn.web_forms import EventForm, EventSignalForm

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
        "registrationID": ven.registrationID,
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
    report_devices = {}
    for report in reports:
        report_data_points[report] = DataPoint.find({DataPoint.report(): report._id})
        report_devices['report'] = Device.find({Device.report(): report._id})

    # if request.method == "POST":
    #     register_reports = []
    #     cancel_reports = []
    #     for report, data_points in report_data_points.items():
    #         register_data_points = []
    #         non_registered_data_points = []
    #         change = False
    #         for data_point in data_points:
    #             subscription = True if request.form.get("r{}-{}".format(report.eiReportID, data_point.rID)) else False
    #             if subscription:
    #                 if data_point.subscribed != True:
    #                     change=True
    #                 register_data_points.append(data_point)
    #             else:
    #                 if data_point.subscribed != False:
    #                     change=True
    #                 non_registered_data_points.append(data_point)
    #             data_point.subscribed=subscription
    #             data_point.save()
    #
    #         if change and register_data_points:
    #             report.subscribed = True
    #             register_reports.append((report, register_data_points))
    #         elif change:
    #             cancel_reports.append(report)
    #             report.subscribed = False
    #         report.save()
    #     if register_reports:
    #         createReport = OadrCreateReport()
    #         report_types = [{"reportId":x.eiReportID,
    #                          "specifierId": x.specifierID,
    #                          "data_points":[
    #                              {
    #                                  'rid': d.rID,
    #                                  'reading_type': d.readingType
    #                              } for d in y]
    #                         } for x, y in register_reports]
    #         params = {
    #             "requestID": "0",
    #             "report_types": report_types
    #         }
    #         response = send_message(createReport, ven, params)
    #         #TODO do something with the response if required by the protocol. When oadrPoll response will be None
    #     if cancel_reports:
    #         cancelReport = OadrCancelReport()
    #         params = {
    #             "requestID": "0",
    #             "cancelReport": [x.eiReportID for x in cancel_reports],
    #             "followUp": False,
    #         }
    #         response = send_message(cancelReport, ven, params)
    #         #TODO do something with the response if required by the protocol. When oadrPoll response will be None
    return render_template("web/ven/ven_reports.html", ven=ven, report_data_points=report_data_points, report_devices=report_devices)

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
            event = Event(data['eventID'], data['priority'], data['marketContext'], data['eventStatus'], True if 'testEvent' in data and data['testEvent']=="y" else False ,data['vtnComment'],"", data['dtstart'], data['duration'],data['tolerance'], data['eiNotification'], data['eiRampUp'], data['eiRecovery'])
            event.save()
            return redirect(url_for("visual.view_vtn_events"))
        else:
            print("A")
    return render_template("web/events/create_events.html", form=form)

@web.route("/vtn_create_event_signal/<eventID>",  methods=['GET', 'POST'])
def create_vtn_event_signals(eventID):
    form=EventSignalForm()
    if request.method=="POST":
        form = EventSignalForm(request.form)
        print(request.form)
        if form.validate():
            data = request.form
            event = Event.find_one({Event.event_id():eventID})
            signal = EventSignal(event._id, data['signalID'], data['signalType'], data['signalName'], "", "")
            signal.save()
            interval = EventInterval(signal._id, data['intervals-0-uid'], data['intervals-0-dtstart'], data['intervals-0-duration'], data['intervals-0-signalPayload'])
            interval.save()
            return redirect(url_for("visual.view_vtn_events"))
        # else:
        #     print("A")
    return render_template("web/events/create_events.html", form=form)