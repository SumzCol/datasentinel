from dataguard.session.data_guard_session import DataGuardSession


def get_session(name: str | None = None, **settings) -> DataGuardSession:
    return DataGuardSession.get_or_create(name, **settings)