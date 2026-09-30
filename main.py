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
            # Riceve il messaggio generico dal socket
            message = await websocket.receive()

            # 1. Se Omi invia dati audio binari (i chunk del microfono)
            if "bytes" in message and message["bytes"]:
                buffer.extend(message["bytes"])

                # Ogni ~3 secondi di audio accumulato (~96KB)
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
                                # Invia la risposta strutturata a Omi
                                response_data = {
                                    "text": transcript_text,
                                    "segments": [
                                        {
                                            "text": transcript_text,
                                            "start": 0.0,
                                            "end": 3.0
                                        }
                                    ]
                                }
                                await websocket.send_text(json.dumps(response_data))

            # 2. Se Omi invia messaggi di controllo in testo (ignora in silenzio per non generare warning)
            elif "text" in message:
                continue

    except WebSocketDisconnect:
        print("Omi Disconnesso")
    except Exception as e:
        print(f"Errore: {str(e)}")
