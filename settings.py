import ast
import os

HOST = os.environ['HOST']
PORT = os.environ['PORT']
VTN_PREFIX = os.environ['VTN_PREFIX']
VEN_PREFIX = os.environ['VEN_PREFIX']
MONGO_URI = os.environ['MONGO_URI']
NOTIFICATION_REST_URL = os.environ['NOTIFICATION_REST_URL']
NOTIFICATION_REST_CERT = os.environ['NOTIFICATION_REST_CERT']
OAUTH_PROVIDERS = ast.literal_eval(os.environ['OAUTH_PROVIDERS'])
CLIENT = os.environ['CLIENT']
SECRET = os.environ['SECRET']
CLIENT_OAUTH = os.environ['CLIENT_OAUTH']