from abc import ABC, abstractmethod
from Transcriptor import Transcriptor
import asyncio
import speech_recognition as sr
import os
from pydub import AudioSegment

class GoogleTranscriptor(Transcriptor):
    async def transcribir(self, audio_path: str) -> str:
        loop = asyncio.get_event_loop()
        recognizer = sr.Recognizer()
        
        # Convertir el audio primero
        converted_path = convertir_a_wav(audio_path)
        
        def sync_transcribe():
            with sr.AudioFile(converted_path) as fuente:
                audio = recognizer.record(fuente)
                return recognizer.recognize_google(audio, language='es-ES')
        
        try:
            result = await loop.run_in_executor(None, sync_transcribe)
            os.remove(converted_path)  # Limpiar archivo temporal
            return result
        except Exception as e:
            if os.path.exists(converted_path):
                os.remove(converted_path)
            raise e

def convertir_a_wav(audio_path: str):
    try:
        audio = AudioSegment.from_file(audio_path)
        audio = audio.set_frame_rate(16000).set_channels(1)
        output_path = "temp_converted.wav"
        audio.export(output_path, 
                    format="wav",
                    bitrate="16k",
                    parameters=["-ac", "1", "-ar", "16000"])
        return output_path
    except Exception as e:
        raise ValueError(f"Error convirtiendo audio: {str(e)}")   