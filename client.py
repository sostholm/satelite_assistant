import asyncio
import websockets
import pyaudio
import logging
import subprocess
import os

URI = os.environ.get("URI", "ws://192.168.0.218:9101")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("client")

# Audio configuration
FORMAT = pyaudio.paInt16
CHANNELS = 1
MIC_RATE = 16000  # Must match the server's MIC_RATE
CHUNK_DURATION_MS = 20  # Must match the server's CHUNK_DURATION_MS
CHUNK = int(MIC_RATE * CHUNK_DURATION_MS / 1000)

async def audio_stream():
    uri = URI

    async with websockets.connect(uri) as websocket:
        p = pyaudio.PyAudio()
        stream_in = p.open(format=FORMAT,
                           channels=CHANNELS,
                           rate=MIC_RATE,
                           input=True,
                           frames_per_buffer=CHUNK)

        # Start mpv process
        mpv_command = ["mpv", "--no-cache", "--no-terminal", "--", "fd://0"]
        mpv_process = subprocess.Popen(
            mpv_command,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        try:
            logger.info("Starting audio stream...")
            while True:
                # Read audio from microphone
                data = stream_in.read(CHUNK, exception_on_overflow=False)
                # Send audio data to server
                await websocket.send(data)

                # Check for incoming TTS audio from server
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=0.01)
                    if isinstance(message, bytes):
                        if mpv_process.stdin:
                            mpv_process.stdin.write(message)
                            mpv_process.stdin.flush()
                except asyncio.TimeoutError:
                    pass
                except Exception as e:
                    logger.error(f"Error receiving audio: {e}")
        except Exception as e:
            logger.error(f"Error in audio stream: {e}")
        finally:
            stream_in.stop_stream()
            stream_in.close()
            p.terminate()
            if mpv_process.stdin:
                mpv_process.stdin.close()
            mpv_process.wait()
            logger.info("Audio streams closed.")

asyncio.run(audio_stream())
