class OadrReport():
    def create(self, reportRequestId, reportSpecifierID, created, reportID, dt_start, duration, intervals):
        raise NotImplementedError("This is an abstract class")
    def parse(self, oadrReport):
        raise NotImplementedError("This is an abstract class")