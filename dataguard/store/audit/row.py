from typing import Any


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
        return f"AuditRow({', '.join(f'{k}={v}' for k, v in self.__dict__.items())})"