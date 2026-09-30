import io
import os
import json
import wave
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()
COHERE_API_KEY = os.getenv("COHERE_API_KEY")
COHERE_URL = "https://api.cohere.com/v2/audio/transcriptions"
COHERE_MODEL = "cohere-transcribe-03-2026"

# Se Omi continua a dare "Unsupported message type: String",
# metti True (frame binario). Se invece va bene il testo, metti False.
SEND_AS_BINARY = False

CHUNK_SECONDS = 3


def pcm_to_wav(pcm: bytes, rate: int) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)  # PCM16
        w.setframerate(rate)
        w.writeframes(pcm)
    return buf.getvalue()


@app.get("/")
def home():
    return {"status": "Omi Cohere Proxy Running"}


@app.websocket("/cohere-live-stt")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    language = websocket.query_params.get("language", "it")
    try:
        rate = int(websocket.query_params.get("sample_rate", 16000))
    except ValueError:
        rate = 16000

    chunk_bytes = rate * 2 * CHUNK_SECONDS  # 16-bit mono
    buffer = bytearray()
    elapsed = 0.0  # secondi di audio già trascritti

    print(f"Connesso: language={language}, sample_rate={rate}")

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            while True:
                message = await websocket.receive()

                if message["type"] == "websocket.disconnect":
                    break

                data = message.get("bytes")
                if not data:
                    # messaggi di testo/controllo: ignorati
                    continue

                buffer.extend(data)
                if len(buffer) < chunk_bytes:
                    continue

                pcm = bytes(buffer)
                buffer.clear()
                duration = len(pcm) / (rate * 2)
                t0, t1 = elapsed, elapsed + duration
                elapsed = t1

                response = await client.post(
                    COHERE_URL,
                    headers={"Authorization": f"Bearer {COHERE_API_KEY}"},
                    files={"file": ("speech.wav", pcm_to_wav(pcm, rate), "audio/wav")},
                    data={"model": COHERE_MODEL, "language": language},
                )

                if response.status_code != 200:
                    print(f"Cohere {response.status_code}: {response.text[:300]}")
                    continue

                text = response.json().get("text", "").strip()
                if not text:
                    continue

                payload = json.dumps(
                    {
                        "segments": [
                            {
                                "text": text,
                                "speaker": "SPEAKER_00",
                                "start": round(t0, 2),
                                "end": round(t1, 2),
                            }
                        ]
                    }
                )

                if SEND_AS_BINARY:
                    await websocket.send_bytes(payload.encode("utf-8"))
                else:
                    await websocket.send_text(payload)

        except WebSocketDisconnect:
            print("Omi disconnesso")
        except Exception as e:
            print(f"Errore: {e!r}")
                
