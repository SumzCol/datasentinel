from abc import abstractmethod


class AuditStoreError(Exception):
    pass


class AuditStoreManagerError(Exception):
    pass


class AuditStoreAlreadyExistsError(AuditStoreManagerError):
    pass


class AuditStoreNotFoundError(AuditStoreManagerError):
    pass


class AuditRow:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    @property
    def columns(self):
        return list(self.__dict__.keys())

    def to_dict(self):
        return self.__dict__

    def __getitem__(self, item):
        return self.__dict__[item]

    def __iter__(self):
        return iter(self.__dict__.values())

    def __repr__(self):
        return f"CustomRow({', '.join(f'{k}={v}' for k, v in self.__dict__.items())})"


class AbstractAuditStore:
    def __init__(self, name: str, disabled: bool):
        self._disabled = disabled
        self._name = name

    @property
    def name(self):
        return self._name

    @property
    def disabled(self):
        return self._disabled

    @abstractmethod
    def append(self, row: AuditRow):
        """Append a row to the audit store."""