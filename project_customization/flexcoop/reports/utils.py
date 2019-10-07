import re


def parse_rid(rid):
    """function that parses the information in the RID"""
    phisical_device, pdn, groupID, spaces, load, ln, metric = re.split("(?<!_)_(?!_)", rid)
    spaces = spaces.split("__")
    return "{}_{}".format(phisical_device, pdn), groupID, spaces, "{}_{}".format(load, ln), metric
