class OadrReport():
    def create(self, *args, **kwargs):
        raise NotImplementedError("This is an abstract class")
    def parse(self, oadrReport, *args, **kwargs):
        raise NotImplementedError("This is an abstract class")