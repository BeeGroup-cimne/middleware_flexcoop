import re
import uuid
import requests

from settings import OAUTH_PROVIDERS, CLIENT, SECRET, CLIENT_OAUTH

status_mapping = {
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
    return phisical_device, pdn, groupID, spaces, load, ln, metric

def get_id_from_rid(rid):
    return rid.rsplit('_', 1)[0]

def generate_UUID():
    return str(uuid.uuid1())

def convert_snake_case(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def convert_camel_case(name):
    components = name.split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return ''.join(x.title() for x in components)

def get_middleware_token():
    client = CLIENT
    secret = SECRET
    login = {'grant_type': 'client_credentials', 'client_id': client, 'client_secret': secret}
    response = requests.post("{}/token".format(OAUTH_PROVIDERS[CLIENT_OAUTH]['url']), data=login, verify=OAUTH_PROVIDERS[CLIENT_OAUTH]['cert'])
    if response.ok:
        return response.json()['access_token']
    else:
        raise Exception("Oauth client not found")