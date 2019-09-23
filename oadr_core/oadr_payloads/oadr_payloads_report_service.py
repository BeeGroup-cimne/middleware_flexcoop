from datetime import datetime

from oadr_core.oadr_payloads.oadr_payloads_general import ELEMENTS, eiResponse, NAMESPACES, pretty_print_xml


# define custom and standard reports
#from oadr_core.oadr_payloads.reports.telemetry_usage import telemetry_usage_report
from oadr_core.oadr_payloads.reports.reports_installed import available_reports


def metadata_telemetry_usage_report(reportRequestId, reportSpecifierID, created, reportID, duration, datapoints):
    """
    Generates a metadata report for telemetry usage
    :param reportRequestId: The requestID
    :param reportSpecifierID: The report specifier id
    :param created: the datetime of creation
    :param reportID: the report id
    :param duration: the duration of this report(the amount of data that can be stored)
    :param datapoints: a list of datapoint dictionaries, each one with the information of the datapoint:
                        {id: id of the datapoint,
                        data_soucrce: the source of the data,
                        itembase: the characteristics of the data(see documentation)
                        min_period: the minimum period,
                        max_period: the maximum period,
                        market_context: the market context
                        }
    :return:
    """
    oadr_report_element = ELEMENTS['oadr'].oadrReport(
        ELEMENTS['xcal'].duration(ELEMENTS['xcal'].duration(duration)),
        ELEMENTS['ei'].eiReportID(reportID),
    )
    for datapoint in datapoints:
        report_description = ELEMENTS['oadr'].oadrReportDescription(
            ELEMENTS['ei'].rID(datapoint['id']),
            # TODO DEFINE DATA SOURCE STRUCTURE
            #ELEMENTS['ei'].reportDataSource(datapoint['data_source']),
            ELEMENTS['ei'].reportType("usage"),
            # TODO DEFINE item base STRUCTURE
            #ELEMENTS['emix'].itemBase(item_base),
            ELEMENTS['ei'].readingType("Direct Read")
        )
        if 'market_context' in datapoint:
            report_description.append(ELEMENTS['emix'].marketContext(datapoint['market_context']))
        report_description.append(
            ELEMENTS['oadr'].oadrSamplingRate(
                ELEMENTS['oadr'].oadrMinPeriod(datapoint['min_period']),
                ELEMENTS['oadr'].oadrMaxPeriod(datapoint['max_period']),
                ELEMENTS['oadr'].oadrOnChange("true" if datapoint["onChange"] else "false")
            )
        )
        oadr_report_element.append(report_description)

        oadr_report_element.append(ELEMENTS['ei'].reportRequestID(reportRequestId))
        oadr_report_element.append(ELEMENTS['ei'].reportSpecifierID(reportSpecifierID))
        oadr_report_element.append(ELEMENTS['ei'].reportName("METADATA_TELEMETRY_USAGE"))
        oadr_report_element.append(ELEMENTS['ei'].createdDateTime(created))
    return oadr_report_element





def metadata_report(duration, eiReportID, datapoints, reportRequestId, reportSpecifierID, reportName, createdDateTime):

    oadr_report_element = ELEMENTS['oadr'].oadrReport(
        ELEMENTS['xcal'].duration(ELEMENTS['xcal'].duration(duration)),
        ELEMENTS['ei'].eiReportID(eiReportID),
    )

    for datapoint in datapoints:
        report_description = ELEMENTS['oadr'].oadrReportDescription(
            ELEMENTS['ei'].rID(datapoint['rID']),
            ELEMENTS['ei'].reportType(datapoint['reportType']),
            ELEMENTS['ei'].readingType(datapoint['readingType'])
        )
        if 'marketContext' in datapoint and datapoint['marketContext']:
            report_description.append(ELEMENTS['emix'].marketContext(datapoint['marketContext']))

        report_description.append(
            ELEMENTS['oadr'].oadrSamplingRate(
                ELEMENTS['oadr'].oadrMinPeriod(datapoint['oadrMinPeriod']),
                ELEMENTS['oadr'].oadrMaxPeriod(datapoint['oadrMaxPeriod']),
                ELEMENTS['oadr'].oadrOnChange("true" if datapoint["oadrOnChange"] else "false")
            )
        )
        oadr_report_element.append(report_description)

        oadr_report_element.append(ELEMENTS['ei'].reportRequestID(reportRequestId))
        oadr_report_element.append(ELEMENTS['ei'].reportSpecifierID(reportSpecifierID))
        oadr_report_element.append(ELEMENTS['ei'].reportName(reportName))
        oadr_report_element.append(ELEMENTS['ei'].createdDateTime(createdDateTime))
    return oadr_report_element


def oadrReportRequest(reportRequestId, reportSpecifierID, rid_list):
    report_specifier = ELEMENTS['ei'].reportSpecifier(
        ELEMENTS['ei'].reportSpecifierID(reportSpecifierID),
        ELEMENTS['xcal'].granularity(
            ELEMENTS['xcal'].duration("PT1H")
        ),
        ELEMENTS['ei'].reportBackDuration(
            ELEMENTS['xcal'].duration("P1D")
        )
    )

    for rid in rid_list:
        report_specifier.append(
            ELEMENTS['ei'].specifierPayload(
                ELEMENTS['ei'].rID(rid['rid']),
                ELEMENTS['ei'].readingType(rid['reading_type'])
            )
        )

    report_request_element = ELEMENTS['oadr'].oadrReportRequest(
        ELEMENTS['ei'].reportRequestID(reportRequestId),
        report_specifier
    )

    return report_request_element


def oadrRegisteredReport(code, description, requestID, report_types, venID):
    oadr_registered_element = ELEMENTS['oadr'].oadrRegisteredReport(
        eiResponse(code, description, requestID)
    )
    for report in report_types:
        oadr_registered_element.append(oadrReportRequest(report['reportId'], report['specifierId'], report['data_points']))

    oadr_registered_element.append(
        ELEMENTS['ei'].venID(venID)
    )
    return oadr_registered_element


def oadrRegisterReport(requestID, reportRequestId, venID, report_dic):
    oadr_register_element = ELEMENTS['oadr'].oadrRegisterReport(
        ELEMENTS['pyld'].requestID(requestID)
    )
    for rep in report_dic:

        oadr_register_element.append(
            metadata_report(rep['duration'], rep['eiReportID'], rep['data_points'], rep['reportRequestID'], rep['reportSpecifierID'], rep['reportName'], rep['createdDateTime'])
        )
        # if rep['reportName'] == "METADATA_TELEMETRY_USAGE":
        #     oadr_register_element.append(
        #         metadata_telemetry_usage_report(reportRequestId if reportRequestId else 0, rep['specifierID'],
        #                                         datetime.now().isoformat(), rep['reportID'], rep['duration'], rep['data_points'])
        #     )
    if venID:
        oadr_register_element.append(ELEMENTS['ei'].venID(venID))

    if reportRequestId:
        oadr_register_element.append(ELEMENTS['ei'].reportRequestID(reportRequestId))


    return oadr_register_element

def oadrCancelReport(cancelReport, requestID, venID, followUp):
    oadr_cancel = ELEMENTS['oadr'].oadrCancelReport(
        ELEMENTS['pyld'].requestID(requestID),
    )
    if venID:
        oadr_cancel.append(ELEMENTS['ei'].venID(venID))
    for cancel in cancelReport:
        oadr_cancel.append(ELEMENTS['oadr'].oadrReportRequestID(cancel))
    oadr_cancel.append(
        ELEMENTS['pyld'].reportToFollow(followUp)
    )
    return oadr_cancel

def oadrUpdateReport(requestID, reports_dic, venID):
    oadr_update_element = ELEMENTS['oadr'].oadrUpdateReport(
        ELEMENTS['pyld'].requestID(requestID),
    )
    if venID:
        oadr_update_element.append(ELEMENTS['ei'].venID(venID))
    for report in reports_dic:
        try:
            report = available_reports[report['type']]
            oadr_update_element.append(report.create(requestID, report['specifierID'], datetime.utcnow().isoformat(), report['reportID'],
                                                                  report['dtstart'], report['duration'], report['intervals']))
        except Exception as e:
            print("The report {} is not supported and is being ignored".format(report['type']))
    return oadr_update_element

def oadrUpdatedReport(code, description, requestID, cancelReport, venID):
    oadr_updated_element = ELEMENTS['oadr'].oadrUpdatedReport(
        eiResponse(code, description, requestID),
        ELEMENTS['ei'].venID(venID)
    )

    if cancelReport:
        oadr_updated_element.append(
            oadrCancelReport(cancelReport, requestID)
        )
    return oadr_updated_element



def oadrCanceledReport(code, description, requestID, pending_reports, venID):
    oadr_pending = ELEMENTS['oadr'].oadrPendingReports()
    for pending in pending_reports:
        oadr_pending.append(
            ELEMENTS['ei'].reportRequestID(pending)
        )
    oadr_canceled_element = ELEMENTS['oadr'].oadrCanceledReport(
        eiResponse(code, description, requestID),
        oadr_pending,
        ELEMENTS['ei'].venID(venID)
    )
    return oadr_canceled_element


def oadrCreateReport(requestID, report_types, venID):
    oadr_create_element = ELEMENTS['oadr'].oadrCreateReport(
        ELEMENTS['pyld'].requestID(requestID)
    )
    for i, report in enumerate(report_types):
        oadr_create_element.append(oadrReportRequest(str(i), report['specifierId'], report['data_points']))

    if venID:
        oadr_create_element.append(ELEMENTS['ei'].venID(venID))



    return oadr_create_element


def oadrCreatedReport(code, description, requestID, pending_reports, venID):
    oadr_pending = ELEMENTS['oadr'].oadrPendingReports()
    for pending in pending_reports:
        oadr_pending.append(
            ELEMENTS['ei'].reportRequestID(pending)
        )
    oadr_created_element = ELEMENTS['oadr'].oadrCreatedReport(
        eiResponse(code, description, requestID),
        oadr_pending,
        ELEMENTS['ei'].venID(venID)
    )
    return oadr_created_element

