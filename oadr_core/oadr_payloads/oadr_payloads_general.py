#XML GENERATION CONFIGURATION
from lxml import etree
from lxml.builder import ElementMaker

NAMESPACES = {
    'xs': "http://www.w3.org/2001/XMLSchema",
    'dsig11': "http://www.w3.org/2009/xmldsig11#",
    'ds': "http://www.w3.org/2000/09/xmldsig#",
    'oadr': "http://openadr.org/oadr-2.0b/2012/07",
    'clm5ISO42173A': "urn:un:unece:uncefact:codelist:standard:5:ISO42173A:2010-04-07",
    'pyld': "http://docs.oasis-open.org/ns/energyinterop/201110/payloads",
    'ei': "http://docs.oasis-open.org/ns/energyinterop/201110",
    'scale': "http://docs.oasis-open.org/ns/emix/2011/06/siscale",
    'emix': "http://docs.oasis-open.org/ns/emix/2011/06",
    'strm': "urn:ietf:params:xml:ns:icalendar-2.0:stream",
    'xcal': "urn:ietf:params:xml:ns:icalendar-2.0",
    'power': "http://docs.oasis-open.org/ns/emix/2011/06/power",
    'gb': "http://naesb.org/espi",
    'gml': "http://www.opengis.net/gml/3.2",
    'atom': "http://www.w3.org/2005/Atom",
    'xml': "http://www.w3.org/XML/1998/namespace",
    'xsi': "http://www.w3.org/2001/XMLSchema-instance",

}

ELEMENTS = {
    k: ElementMaker(namespace=v) for k, v in NAMESPACES.items()
}


def pretty_print_xml(document):
    """
    Print xml document in the proper way
    :param document: the document to print
    """
    print(str(etree.tostring(document, pretty_print=True)).replace("\\n", '\n'))


def localize_xml_namespace(root, xsi=NAMESPACES['xsi'],
                           xsi_location="http://openadr.org/oadr-2.0b/2012/07 schema/oadr_20b.xsd"):
    """
    ADDS the xsi:schemaLocation at the xml payload
    :param root: the document to add the xsi:schemaLocation
    :param xsi: the namespace of XSI
    :param xsi_location: the location address
    :return: the root document with the xsi:schemaLocation attribute
    """
    root.attrib['{{{pre}}}schemaLocation'.format(pre=xsi)] = xsi_location
    return root


def oadrPayload(oadr_content):
    root = ElementMaker(namespace=NAMESPACES['oadr'], nsmap=NAMESPACES)
    oadrPayload = root.oadrPayload(
        ELEMENTS['oadr'].oadrSignedObject(
            oadr_content
        )
    )
    return localize_xml_namespace(oadrPayload)


def eiResponse(code, description, requestID):
    """
    Generates a eiResponse document
    :param code: the request response code
    :param description: the description of the respones code
    :param requestID: the request ID of this response
    :return: a eiResponse document
    """
    ei_response_element = ELEMENTS['ei'].eiResponse(
        ELEMENTS['ei'].responseCode(code),
        ELEMENTS['ei'].responseDescription(description),
        ELEMENTS['pyld'].requestID(requestID)
    )
    return ei_response_element


def oadrResponse(response_code, response_description, response_requestId, venID):
    response_element = ELEMENTS['oadr'].oadrResponse(
        eiResponse(response_code, response_description, response_requestId),
        ELEMENTS['ei'].venID(venID)
    )
    return response_element
