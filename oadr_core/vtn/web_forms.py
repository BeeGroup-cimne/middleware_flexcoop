from wtforms import Form, TextAreaField, validators, StringField, SubmitField, SelectField, IntegerField, BooleanField, \
    DateTimeField, FieldList, FormField

class EventIntervalFor(Form):
    uid = StringField('uid:', validators=[validators.required()])
    dtstart = DateTimeField("Start time", validators=[validators.required()])
    duration = StringField('duration:', validators=[validators.required()])
    signalPayload = StringField('signalPayload:', validators=[validators.required()])


class EventSignalForm(Form):
    signalID = StringField('SignalID:', validators=[validators.required()])
    signalType = StringField('signalType:', validators=[validators.required()])
    signalName = StringField('signalName:', validators=[validators.required()])
    itemBase = StringField('itemBase:', validators=[validators.optional()])
    currentValue = StringField('currentValue:', validators=[validators.optional()])
    intervals = FieldList(FormField(EventIntervalFor), min_entries=1, max_entries=4)


class EventForm(Form):
    eventID = StringField('EventID:', validators=[validators.required("Field is mandatory")])
    priority = IntegerField('Priority:', validators=[validators.optional("Field must be integer")], description="The lower the most priority, 0 being no priority")
    marketContext = StringField("MarketContext", validators=[validators.optional()])
    eventStatus = SelectField(StringField("Status", validators=[validators.required()]), choices=[("FAR", "FAR"),("NEAR", "NEAR"), ("ACTIVE", "ACTIVE"), ("canceled","CANCELED")])
    testEvent = BooleanField("Test", validators=[validators.optional()])
    vtnComment = TextAreaField("Comment", validators=[validators.optional()])
    dtstart = DateTimeField("Start time", validators=[validators.required()], format="%Y-%m-%d %H:%M:%S")
    duration = StringField("Duration", validators=[validators.optional()])
    tolerance = StringField("Tolerance", validators=[validators.optional()])
    eiNotification = StringField("Notification", validators=[validators.optional()])
    eiRampUp = StringField("RampUp", validators=[validators.optional()])
    eiRecovery = StringField("Recovery", validators=[validators.optional()])
    #signal = FieldList(FormField(EventSignalForm), min_entries=4)
