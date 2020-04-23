import datetime
import re
import uuid
import requests

from settings import OAUTH_PROVIDERS, CLIENT, SECRET, CLIENT_OAUTH

status_mapping = {
    "status": "operationState",
    "mode": "mode",
    "temperature": "setPoint",
    "fanspeed": "x-fanspeed",
    "brightness": "setPoint",
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


class ServiceToken(object):
    """
        A singleton instance object providing access to a middleware service token
        via
          token =  ServiceToken():get_token()
        Hardcoded version of rest api one
        The class caches the retrieved access token until 5 minutes before expiration to minimise network traffic
    """
    class _ServiceToken(object):
        def __init__(self):
            self.token = None
            self.exp = None
            self.token_url = "/token"
            self.client = CLIENT
            self.secret = SECRET
            self.oauth = OAUTH_PROVIDERS[CLIENT_OAUTH]['url']
            self.cert = OAUTH_PROVIDERS[CLIENT_OAUTH]['cert']

        def __str__(self):
            return repr(self)

        def get_token(self):
            if self.token:
                if datetime.datetime.utcnow() <= self.exp:
                    return self.token

            login = {'grant_type': 'client_credentials', 'client_id': self.client, 'client_secret': self.secret}
            response = requests.post(self.oauth+"/"+self.token_url, data=login, verify=self.cert)
            if response.ok:
                self.token = response.json()['access_token']
                self.exp = datetime.datetime.utcnow() + datetime.timedelta(minutes=30)
                return self.token
            else:
                raise Exception("Bad Oauth response during ServiceToken().get_token() : "+response.text)

    instance = None

    def __init__(self):
        if not ServiceToken.instance:
            ServiceToken.instance = ServiceToken._ServiceToken()

    def __getattr__(self, name):
        return getattr(self.instance, name)


def get_middleware_token():
    stoken = ServiceToken()
    token = stoken.get_token()
    if token:
        return token
    else:
        raise Exception("Oauth client not found")