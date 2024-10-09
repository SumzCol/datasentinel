from abc import ABC, abstractmethod
from typing import List, Dict, Any


class AbstractBadRecordsDataset(ABC):

    @abstractmethod
    def count(self) -> int:
        pass

    @abstractmethod
    def to_dict(self, top: int = 1000) -> List[Dict[str, Any]]:
        pass