
class InvalidVenException(Exception):
    code = "452"
    description = "Invalid venID"


class InvalidResponseException(Exception):

    def __init__(self, code, description, *args, **kwargs):
        self.code = code
        self.description = description
        super(InvalidResponseException, self).__init__(*args, **kwargs)


class InvalidReportException(Exception):
    code = "452"
    description = ""

    def __init__(self, *args, **kwargs):
        super(InvalidReportException, self).__init__(*args, **kwargs)
        self.description = str(self)