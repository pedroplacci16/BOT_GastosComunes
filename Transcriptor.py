from abc import ABC, abstractmethod
class Transcriptor(ABC):
    @abstractmethod
    async def transcribir(self, audio_path: str) -> str:
        pass