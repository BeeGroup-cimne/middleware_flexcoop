import flask
import functools
from flask import Blueprint, jsonify, request

from oadr_core.vtn.server_blueprint import send_message
from oadr_core.vtn.services.ei_event_service import OadrDistributeEvent
from project_customization.flexcoop.authentication import AuthenticationException, JWTokenAuth
from project_customization.flexcoop.models import VEN, Event

notification = Blueprint('notification', __name__)

def login_required(view):
    @functools.wraps(view)
    def wrapped_view(**kwargs):
        if not 'Authorization' in request.headers:
            flask.abort(403, "Provide credentials")
        token = request.headers['Authorization']
        authentication = JWTokenAuth()
        if authentication.check_auth(token):
            return view(**kwargs)
        else:
            flask.abort(403, "Credentials are not valid")

    return wrapped_view

@notification.route("/events/<ven_id>", methods=['GET'])
@login_required
def notification_events(ven_id):
    events = Event.find({Event.ven_id(): ven_id})
    if not events:
        return jsonify({"notification": "No events"})
    send_message(OadrDistributeEvent(), VEN.find_one({VEN.ven_id(): ven_id}),
                 {'event_list': events, "requestID": "1"})
    print("event_sent")
    for e in events:
        e.delete()
    return jsonify({"notification": "OK"})