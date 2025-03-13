from abc import ABC, abstractmethod
from Transcriptor import Transcriptor
import whisper
import asyncio

class WhisperTranscriptor(Transcriptor):
    def __init__(self, model_name: str = "base"):
        self.model = whisper.load_model(model_name)
    
    async def transcribir(self, audio_path: str) -> str:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, self.model.transcribe, audio_path)
        return result['text']