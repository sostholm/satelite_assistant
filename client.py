import asyncio
import websockets
import pyaudio
import logging
import subprocess
import os

URI = os.environ.get("URI", "ws://192.168.0.218:9100")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("client")

# Audio configuration
FORMAT = pyaudio.paInt16
CHANNELS = 1
MIC_RATE = 16000  # Must match the server's MIC_RATE
CHUNK_DURATION_MS = 20  # Must match the server's CHUNK_DURATION_MS
CHUNK = int(MIC_RATE * CHUNK_DURATION_MS / 1000)

async def listen(websocket):
    p = pyaudio.PyAudio()
    for i in range(p.get_device_count()):
        dev_info = p.get_device_info_by_index(i)
        print(f"Device {i}: {dev_info['name']} (max input channels={dev_info['maxInputChannels']})")
    loop = asyncio.get_running_loop()
    logger.info("Listening...")

    # Create a queue to hold audio data
    audio_queue = asyncio.Queue()

    def callback(in_data, frame_count, time_info, status):
        # This function is called in a separate thread by PyAudio
        # Put the audio data into the queue
        asyncio.run_coroutine_threadsafe(audio_queue.put(in_data), loop)
        return (None, pyaudio.paContinue)

    stream_in = p.open(
        format=FORMAT,
        channels=CHANNELS,
        rate=MIC_RATE,
        input=True,
        frames_per_buffer=CHUNK,
        input_device_index=2,
        stream_callback=callback
    )

    stream_in.start_stream()

    try:
        while True:
            # Get audio data from the queue
            data = await audio_queue.get()
            # Send audio data to server
            await websocket.send(data)
    except Exception as e:
        logger.error(f"Error in listen function: {e}")
    finally:
        stream_in.stop_stream()
        stream_in.close()
        p.terminate()

async def speak(websocket):
    # Start mpv process
    mpv_command = ["mpv", "--no-cache", "--no-terminal", "--", "fd://0"]
    mpv_process = subprocess.Popen(
        mpv_command,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    loop = asyncio.get_running_loop()

    # Create a queue to hold incoming audio data
    audio_queue = asyncio.Queue()

    # Function to write data to mpv's stdin in a separate thread
    def write_to_mpv(data):
        if mpv_process.stdin:
            mpv_process.stdin.write(data)
            mpv_process.stdin.flush()

    try:
        # Task to read from websocket and put data into queue
        async def read_from_websocket():
            try:
                while True:
                    message = await websocket.recv()
                    if isinstance(message, bytes):
                        await audio_queue.put(message)
            except Exception as e:
                logger.error(f"Error receiving audio: {e}")

        # Task to write data from queue to mpv
        async def write_to_mpv_task():
            try:
                while True:
                    data = await audio_queue.get()
                    await loop.run_in_executor(None, write_to_mpv, data)
            except Exception as e:
                logger.error(f"Error writing to mpv: {e}")

        # Run both tasks concurrently
        await asyncio.gather(read_from_websocket(), write_to_mpv_task())

    except Exception as e:
        logger.error(f"Error in speak function: {e}")
    finally:
        if mpv_process.stdin:
            mpv_process.stdin.close()
        mpv_process.wait()
        logger.info("Speaker process closed.")

async def main():
    while True:
        async with websockets.connect(URI) as websocket:
            await asyncio.gather(listen(websocket), speak(websocket))
        
        logger.info("Connection closed, retrying in 5 seconds...")
        await asyncio.sleep(5)
        

if __name__ == "__main__":
    asyncio.run(main())
