"""
Omi <-> Cohere STT bridge.
Omi (custom STT, request_type "streaming") 
opens a WebSocket to /cohere-live-stt
and sends raw PCM16 mono audio as binary 
frames.
This server buffers the audio in chunks, 
sends each chunk to Cohere as WAV,
and replies to Omi with JSON: {"segments": 
[{"text", "speaker", "start", "end"}]}
Environment variables (Render -> 
Environment):
  COHERE_API_KEY   required
  COHERE_MODEL     default: cohere
transcribe-03-2026
  SEND_MODE        "binary" (default) or 
"text"  -> how the JSON reaches Omi
  CHUNK_SECONDS    default: 4
  SILENCE_RMS      default: 300  (chunks 
quieter than this are skipped)
Start command: uvicorn main:app --host 
0.0.0.0 --port $PORT
"""
import asyncio
import io
import json
import os
import wave
from array import array

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

VERSION = "v3"
COHERE_API_KEY = os.getenv("COHERE_API_KEY", "").strip()
COHERE_MODEL = os.getenv("COHERE_MODEL", "cohere-transcribe-03-2026").strip()
COHERE_URL = "https://api.cohere.com/v2/audio/transcriptions"
SEND_MODE = os.getenv("SEND_MODE", "binary").lower()
CHUNK_SECONDS = float(os.getenv("CHUNK_SECONDS", "4"))
SILENCE_RMS = float(os.getenv("SILENCE_RMS", "300"))
MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", "20"))

app = FastAPI()


@app.get("/")
def home():
    return {
        "status": "Omi Cohere Proxy Running",
        "version": VERSION,
        "send_mode": SEND_MODE,
        "chunk_seconds": CHUNK_SECONDS,
        "cohere_key_set": bool(COHERE_API_KEY),
    }


def pcm_to_wav(pcm: bytes, rate: int) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(rate)
        wav_file.writeframes(pcm)
    return buf.getvalue()


def rms(pcm: bytes) -> float:
    """Rough loudness of PCM16 audio (subsampled for speed)."""
    if len(pcm) < 2:
        return 0.0

    samples = array("h")
    samples.frombytes(pcm[: len(pcm) - (len(pcm) % 2)])

    if not samples:
        return 0.0

    step = 8
    picked = samples[::step]
    return (sum(s * s for s in picked) / len(picked)) ** 0.5


async def send_to_omi(ws: WebSocket, payload: dict) -> None:
    raw = json.dumps(payload, ensure_ascii=False)
    if SEND_MODE == "text":
        await ws.send_text(raw)
    else:
        await ws.send_bytes(raw.encode("utf-8"))


async def transcribe(client: httpx.AsyncClient, pcm: bytes, rate: int, lang: str) -> str:
    resp = await client.post(
        COHERE_URL,
        headers={"Authorization": f"Bearer {COHERE_API_KEY}"},
        files={"file": ("speech.wav", pcm_to_wav(pcm, rate), "audio/wav")},
        data={"model": COHERE_MODEL, "language": lang},
    )

    if resp.status_code != 200:
        print(f"[cohere] {resp.status_code}: {resp.text[:300]}")
        return ""

    try:
        return (resp.json().get("text") or "").strip()
    except Exception as exc:
        print(f"[cohere] invalid json: {exc!r}")
        return ""


@app.websocket("/cohere-live-stt")
async def cohere_live_stt(ws: WebSocket):
    await ws.accept()

    if not COHERE_API_KEY:
        await ws.send_text(json.dumps({"error": "COHERE_API_KEY is not set"}))
        await ws.close(code=1011)
        return

    lang = ws.query_params.get("language", "it")
    try:
        rate = int(ws.query_params.get("sample_rate", "16000"))
    except ValueError:
        rate = 16000

    chunk_bytes = int(rate * 2 * CHUNK_SECONDS)
    queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)

    print(f"[ws] connected lang={lang} rate={rate} mode={SEND_MODE} version={VERSION}")

    async def worker():
        async with httpx.AsyncClient(timeout=30) as client:
            while True:
                item = await queue.get()
                try:
                    if item is None:
                        return

                    pcm, t0, t1 = item
                    text = await transcribe(client, pcm, rate, lang)

                    if not text:
                        continue

                    payload = {
                        "segments": [
                            {
                                "text": text,
                                "speaker": "SPEAKER_00",
                                "start": round(t0, 2),
                                "end": round(t1, 2),
                            }
                        ]
                    }

                    print(f"[send:{SEND_MODE}] {payload}")
                    await send_to_omi(ws, payload)
                except Exception as exc:
                    print(f"[worker] error: {exc!r}")
                    return
                finally:
                    queue.task_done()

    worker_task = asyncio.create_task(worker())
    buffer = bytearray()
    elapsed = 0.0

    try:
        while True:
            msg = await ws.receive()
            if msg["type"] == "websocket.disconnect":
                break

            data = msg.get("bytes")
            if data is None:
                continue  # ignore text/control frames

            buffer.extend(data)

            while len(buffer) >= chunk_bytes:
                pcm = bytes(buffer[:chunk_bytes])
                del buffer[:chunk_bytes]

                dur = len(pcm) / (rate * 2)
                t0, t1 = elapsed, elapsed + dur
                elapsed = t1

                if rms(pcm) >= SILENCE_RMS:
                    await queue.put((pcm, t0, t1))

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        print(f"[ws] error: {exc!r}")
    finally:
        # Flush any leftover buffer.
        if buffer:
            pcm = bytes(buffer)
            dur = len(pcm) / (rate * 2)
            t0, t1 = elapsed, elapsed + dur
            if rms(pcm) >= SILENCE_RMS:
                try:
                    await queue.put((pcm, t0, t1))
                except Exception:
                    pass

        await queue.put(None)

        try:
            await asyncio.wait_for(worker_task, timeout=10)
        except asyncio.TimeoutError:
            worker_task.cancel()

        print("[ws] closed")
