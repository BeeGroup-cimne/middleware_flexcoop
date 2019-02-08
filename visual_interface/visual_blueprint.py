from flask import Blueprint, render_template

from oadr_core.vtn.models import VEN, MetadataReportSpec

web = Blueprint('visual', __name__, template_folder='templates')

@web.route("/ven", methods=['GET'])
def view_ven():
    return render_template("web/ven_list.html", ven_list=VEN.find({}))

@web.route("/ven_reports/<venID>", methods=['GET'])
def view_reports(venID):
    ven = VEN.find_one({VEN.venID():venID})
    reports = MetadataReportSpec.find({MetadataReportSpec.ven():venID})
    return render_template("web/ven_reports.html", ven=ven, report_list=reports)

