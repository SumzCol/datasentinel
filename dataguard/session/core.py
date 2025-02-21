from dataguard.core import DataGuardError


class DataGuardSessionError(DataGuardError):
    pass


class SessionAlreadyExistsError(DataGuardSessionError):
    pass


class SessionNotSpecifiedError(DataGuardSessionError):
    pass
