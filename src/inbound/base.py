from abc import ABC, abstractmethod
from typing import Dict, Any

class InboundSource(ABC):

    @abstractmethod
    def fetch(self) -> Dict[str, Any]:
        """Fetches a message and returns dict(sender, tenant, subject, body)"""
        pass
