from dataguard.session.validation_session import ValidationSession


def get_session(name: str | None = None, **settings) -> ValidationSession:
    return ValidationSession.get_or_create(name, **settings)