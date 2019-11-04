from datetime import datetime, timedelta
from urllib.parse import urlsplit

import requests
from jwt import (
    JWT,
    jwk_from_dict
)
from flask import request
from requests import RequestException

from settings import OAUTH_PROVIDERS


class KeyCache(object):
    class _KeyCache(object):
        def __init__(self, providers, update_key_freq):
            self.providers = providers
            self.keys = []
            self.last_time = None
            self.update_key_time = update_key_freq
            self.get_keys()

        def get_keys(self):
            if self.last_time and self.keys and self.last_time + self.update_key_time >= datetime.utcnow():
                return self.keys
            self.keys = []
            self.last_time = datetime.utcnow()
            for provider, provider_info in self.providers.items():
                try:
                    p_info = requests.get("{}/.well-known/openid-configuration/".format(provider_info['url']), verify=provider_info['cert'])
                    if p_info.ok:
                        p_info=p_info.json()
                        key_url = p_info["jwks_uri"]
                        key_list = requests.get(key_url, verify=provider_info['cert']).json()
                        for key in key_list['keys']:
                            self.keys.append({"key": jwk_from_dict(key), "iss": provider_info['url'], "kid": key['kid']})
                        print("Provider {} correctly configured".format(provider))
                    else:
                        print("Error connecting with the provider {}: provided returned {}".format(provider,p_info.reason))
                except RequestException as e:
                    print("Error connecting with the provider {}: provider not found".format(provider))
                except TypeError as e:
                    print("Error connecting with the provider {}: incorrect format of provider key".format(provider))
            if not self.keys:
                raise ProviderNotFoundException("No provider found")
            return self.keys

        def __str__(self):
            return repr(self)

    instance = None

    def __init__(self, providers, update_key_freq):
        if not KeyCache.instance:
            KeyCache.instance = KeyCache._KeyCache(providers, update_key_freq)

    def __getattr__(self, name):
        return getattr(self.instance, name)


class JWTokenAuth():
    def check_auth(self, token):
        # TODO: konami code!! Remove when release
        # TODO:____________KONAMI CODE START______________________
        sub = None
        issuer = None
        role = None
        if token == "UUDDLRLRBA1":
            role = "prosumer"
            sub = "11111111-1111-1111-1111-111111111111"
            issuer = "http://217.182.160.171:9042"
        if token == "UUDDLRLRBA11":
            role = "prosumer"
            sub = "11111111-1111-1111-1111-111111111112"
            issuer = "http://217.182.160.171:9043"
        if token == "UUDDLRLRBA2":
            role = "aggregator"
            sub = "22222222-2222-2222-2222-222222222222"
            issuer = "http://217.182.160.171:9043"
        if token == "UUDDLRLRBA3":
            role = "service"
            sub = "33333333-3333-3333-3333-333333333333"
            issuer = ""
        # TODO:____________KONAMI CODE END______________________
        if sub is None:
            jwt = JWT()
            keys_cache = KeyCache(OAUTH_PROVIDERS, timedelta(minutes=10))
            keys = keys_cache.get_keys()
            user_info = None
            key = None
            for key in keys:
                try:
                    user_info = jwt.decode(token, key['key'])
                    key = key
                    break
                except Exception as e:  # todo catch only corresponding exceptions here
                    pass
            if not user_info:
                return False
            else:
                issuer = user_info['iss']
                expiration = datetime.utcfromtimestamp(user_info['exp'])
                role = user_info['role']
                sub = user_info['sub']
                now_time = datetime.utcnow()
                if expiration < now_time:
                    return False
                if issuer != key['iss']:
                    return False
        if sub is None:
            raise AuthenticationException("No role was detected, this should not happen")

        if issuer is None:  # make sure the code above has catched all conditions
            raise AuthenticationException("No issuer was detected, this should not happen")

        if role is None:
            raise AuthenticationException("No role was detected, this should not happen")

        request.role = role
        request.account_id = sub
        request.aggregator_id = urlsplit(issuer).netloc
        return True

class AuthenticationException(Exception):
    pass

class ProviderNotFoundException(Exception):
    pass
