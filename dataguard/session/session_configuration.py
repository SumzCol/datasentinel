class SessionConfiguration:
    def __init__(self, **settings):
        self.settings = settings

    def get(self, key):
        return self.settings.get(key)

    def exists(self, key):
        return key in self.settings

    def __repr__(self):
        return f"Configuration(settings={self.settings})"
