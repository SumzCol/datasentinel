class DataGuardSessionError(Exception):
    pass

class SessionAlreadyExistsError(DataGuardSessionError):
    pass

class SessionNotSpecifiedError(DataGuardSessionError):
    pass