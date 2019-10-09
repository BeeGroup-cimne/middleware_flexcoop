import re
import uuid


def parse_rid(rid):
    """function that parses the information in the RID"""
    phisical_device, pdn, groupID, spaces, load, ln, metric = re.split("(?<!_)_(?!_)", rid)
    spaces = spaces.split("__")
    return "{}{}".format(phisical_device, pdn), groupID, spaces, "{}{}".format(load, ln), metric

def get_id_from_rid(rid):
    return rid.rsplit('_', 1)[0]

def generate_UUID():
    return str(uuid.uuid1())