import os
import json
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()
COHERE_API_KEY = os.getenv("COHERE_API_KEY")

@app.get("/")
def home():
    return {"status": "Omi Cohere Proxy Running"}

@app.websocket("/cohere-live-stt")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    buffer = bytearray()

    try:
        while True:
            # Riceve il messaggio dal socket
            message = await websocket.receive()

            # Se Omi invia dati audio binari (PCM/WAV bytes)
            if "bytes" in message and message["bytes"]:
                data = message["bytes"]
                buffer.extend(data)

                # Quando accumula circa 3 secondi di audio (~96KB)
                if len(buffer) > 96000:
                    audio_chunk = bytes(buffer)
                    buffer.clear()

                    async with httpx.AsyncClient() as client:
                        response = await client.post(
                            "https://api.cohere.com/v2/audio/transcriptions",
                            headers={"Authorization": f"Bearer {COHERE_API_KEY}"},
                            files={"file": ("speech.wav", audio_chunk, "audio/wav")},
                            data={"model": "cohere-transcribe-03-2026", "language": "it"}
                        )
                        
                        if response.status_code == 200:
                            transcript_text = response.json().get("text", "")
                            if transcript_text.strip():
                                # Formato JSON atteso da Omi per la trascrizione live
                                payload = {
                                    "text": transcript_text,
                                    "is_final": True
                                }
                                await websocket.send_text(json.dumps(payload))

            # Se Omi invia messaggi di controllo in formato testo (String), li ignoriamo in silenzio
            elif "text" in message:
                continue

    except WebSocketDisconnect:
        print("Omi disconnesso")
    except Exception as e:
        print(f"Errore Proxy: {str(e)}")
