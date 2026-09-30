import os
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()
COHERE_API_KEY = os.getenv("cohere_JdeYNox4fXJukOZGZfxeDuwO1zNFu6SloV8eY2LP1c23Lr")

@app.get("/")
def home():
    return {"status": "Omi Cohere Proxy Running"}

    @app.websocket("/cohere-live-stt")
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
            buffer = bytearray()

                try:
                        while True:
                                    # Receive audio byte chunks sent by Omi
                                                data = await websocket.receive_bytes()
                                                            buffer.extend(data)

                                                                        # Send to Cohere after accumulating ~3 seconds of audio (~96KB)
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
                                                                                                                                                                                                                                                                                                                                            text = response.json().get("text", "")
                                                                                                                                                                                                                                                                                                                                                                    if text:
                                                                                                                                                                                                                                                                                                                                                                                                # Push text back to Omi UI
                                                                                                                                                                                                                                                                                                                                                                                                                            await websocket.send_json({"text": text, "is_final": True})

                                                                                                                                                                                                                                                                                                                                                                                                                                except WebSocketDisconnect:
                                                                                                                                                                                                                                                                                                                                                                                                                                        print("Omi disconnected")
                                                                                                                                                                                                                                                                                                                                                                                                                                        