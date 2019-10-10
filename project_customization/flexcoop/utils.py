import re
import uuid


statusMapping = {
    "status": "operationState",
    "mode": "mode",
    "temperature": "setPoint",
    "fanspeed": "x-fanspeed",
    "brigthness": "setPoint",
    "colorTemperature": "x-color",
    "color": "setPoint",
    "switchBinary": "operationState"
}

def parse_rid(rid):
    """function that parses the information in the RID"""
    phisical_device, pdn, groupID, spaces, load, ln, metric = re.split("(?<!_)_(?!_)", rid)
    spaces = spaces.split("__")
    return "{}{}".format(phisical_device, pdn), groupID, spaces, "{}{}".format(load, ln), metric

def get_id_from_rid(rid):
    return rid.rsplit('_', 1)[0]

def generate_UUID():
    return str(uuid.uuid1())

def convert_snake_case(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()