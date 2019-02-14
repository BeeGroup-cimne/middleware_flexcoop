from flask import Blueprint, render_template, request

from oadr_core.vtn.server_blueprint import send_message
from oadr_core.vtn.models import VEN, MetadataReportSpec
from oadr_core.vtn.services.ei_report_service import OadrCreateReport, OadrCancelReport

web = Blueprint('visual', __name__, template_folder='templates')

@web.route("/ven", methods=['GET'])
def view_ven():
    return render_template("web/ven_list.html", ven_list=VEN.find({}))

@web.route("/ven_reports/<venID>", methods=['GET','POST'])
def view_reports(venID):
    ven = VEN.find_one({VEN.venID(): venID})
    reports = MetadataReportSpec.find({MetadataReportSpec.ven(): venID})
    if request.method == "POST":
        register = []
        cancel = []
        for report in reports:
            subscription = True if request.form.get("r{}".format(report.reportID)) else False
            if report.subscribed != subscription:
                if subscription:
                    register.append(report)
                else:
                    cancel.append(report)
                report.subscribed = subscription
                report.save()
        if register:
            createReport = OadrCreateReport()
            params = {
                "reportRequestID": [x.reportID for x in register],
                "reportSpecifierID": [x.specifierID for x in register],
                "requestID": "0"
            }
            response = send_message(createReport, ven, params)
            #TODO do something with the response if required by the protocol. When oadrPoll response will be None
        if cancel:
            cancelReport = OadrCancelReport()
            params = {
                "requestID": "0",
                "cancelReport": [x.reportID for x in cancel],
                "followUp": False,
            }
            response = send_message(cancelReport, ven, params)
            #TODO do something with the response if required by the protocol. When oadrPoll response will be None
    return render_template("web/ven_reports.html", ven=ven, report_list=reports)

