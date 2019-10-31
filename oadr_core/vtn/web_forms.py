from wtforms import Form, TextAreaField, validators, StringField, SubmitField, SelectField, IntegerField, BooleanField, \
    DateTimeField, FieldList, FormField, FloatField, Field

from project_customization.flexcoop.models import VEN


class EventIntervalFor(Form):
    uid = StringField('uid:', validators=[validators.optional()])
    dtstart = DateTimeField("Start time", validators=[validators.optional()], format="%Y-%m-%d %H:%M:%S")
    duration = StringField('duration:', validators=[validators.optional()])
    value = FloatField('signalPayload:', validators=[validators.required()])


class EventSignalForm(Form):
    signalID = StringField('SignalID:', validators=[validators.required()])
    signalType = SelectField(StringField('signalType:', validators=[validators.required()]), choices=[("x-loadControlSetpoint","x-loadControlSetpoint")])
    signalName = SelectField(StringField('signalName:', validators=[validators.required()]), choices=[("LOAD_CONTROL", "LOAD_CONTROL")])
    currentValue = FloatField('currentValue:', validators=[validators.optional()])
    target = StringField("DER_target", validators=[validators.optional()])
    #


class EventDescription(Form):
    priority = IntegerField('Priority:', validators=[validators.optional("Field must be integer")], description="The lower the most priority, 0 being no priority")
    marketContext = StringField("MarketContext", validators=[validators.optional()])
    eventStatus = SelectField(StringField("Status", validators=[validators.required()]), choices=[("far", "FAR"),("near", "NEAR"), ("active", "ACTIVE"), ("completed", "COMPLETED"), ("canceled","CANCELED")])
    testEvent = BooleanField("Test", validators=[validators.optional()])
    vtnComment = TextAreaField("Comment", validators=[validators.optional()])
    dtstart = DateTimeField("Start time", validators=[validators.required()], format="%Y-%m-%d %H:%M:%S")
    duration = StringField("Duration", validators=[validators.required()])
    tolerance = StringField("Tolerance", validators=[validators.optional()])
    eiNotification = StringField("Notification", validators=[validators.optional()])
    eiRampUp = StringField("RampUp", validators=[validators.optional()])
    eiRecovery = StringField("Recovery", validators=[validators.optional()])
    target = StringField("Target", validators=[validators.required()])
    responseRequired = SelectField(StringField("Response", validators=[validators.required()]), choices=[("never", "never"),("always", "always")])


class EventForm(Form):
    description = FormField(EventDescription)
    signal = FormField(EventSignalForm)
    interval = FormField(EventIntervalFor)
    ven = SelectField(StringField("VEN_ID", [validators.required()]), choices=[(x.ven_id, x.oadr_ven_name) for x in VEN.find({})])