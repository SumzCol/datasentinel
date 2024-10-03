class SessionConfiguration:
    def __init__(self, **settings):
        self.settings = settings

    def set(self, key, value):
        self.settings[key] = value

    def get(self, key):
        return self.settings.get(key)

    def __repr__(self):
        return f"Configuration(settings={self.settings})"
