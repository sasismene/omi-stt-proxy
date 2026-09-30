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
            # Riceve qualsiasi tipo di messaggio da Omi (Testo o Bytes)
            message = await websocket.receive()

            # 1. Gestione messaggi di Testo / Setup da Omi
            if "text" in message and message["text"]:
                text_data = message["text"]
                try:
                    data_json = json.loads(text_data)
                    # Se Omi invia un messaggio di configurazione/setup, confermiamo la connessione
                    if data_json.get("type") == "setup" or "language" in data_json:
                        await websocket.send_text(json.dumps({"status": "ready"}))
                except json.JSONDecodeError:
                    pass

            # 2. Gestione Audio Binario (Chunk dal microfono)
            elif "bytes" in message and message["bytes"]:
                buffer.extend(message["bytes"])

                # Ogni ~3 secondi di audio accumulato (~96KB) inviamo a Cohere
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
                            transcript = response.json().get("text", "")
                            if transcript.strip():
                                # Formato esatto per visualizzare il testo su Omi
                                await websocket.send_text(json.dumps({
                                    "text": transcript,
                                    "is_final": True
                                }))

    except WebSocketDisconnect:
        print("Omi disconnesso")
    except Exception as e:
        print(f"Errore Proxy: {str(e)}")
        
