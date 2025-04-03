import asyncio
import websockets
import pyaudio
import logging
import subprocess
import os

URI = os.environ.get("URI", "ws://192.168.0.218:9100")
DEVICE_INDEX = int(os.environ.get("DEVICE_INDEX", 2))

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("client")

# Audio configuration
FORMAT = pyaudio.paInt16
CHANNELS = 1
MIC_RATE = 16000  # Must match the server's MIC_RATE
CHUNK_DURATION_MS = 20  # Must match the server's CHUNK_DURATION_MS
CHUNK = int(MIC_RATE * CHUNK_DURATION_MS / 1000)

async def listen(websocket, shutdown_event):
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
        input_device_index=DEVICE_INDEX,
        stream_callback=callback
    )

    stream_in.start_stream()

    try:
        while not shutdown_event.is_set():
            # Get audio data from the queue
            data = await audio_queue.get()
            # Send audio data to server
            try:
                await websocket.send(data)
            except websockets.exceptions.ConnectionClosed:
                logger.error("WebSocket connection closed in listen")
                break
            except Exception as e:
                logger.error(f"Error sending audio data: {e}")
                break
    except Exception as e:
        logger.error(f"Error in listen function: {e}")
    finally:
        stream_in.stop_stream()
        stream_in.close()
        p.terminate()
        logger.info("Microphone stopped")
        shutdown_event.set()  # Signal to other tasks to shutdown

async def speak(websocket, shutdown_event):
    # Start mpv process
    mpv_command = ["mpv", "--no-cache", "--no-terminal", "--", "fd://0"]
    mpv_process = subprocess.Popen(
        mpv_command,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    
    try:
        while not shutdown_event.is_set():
            try:
                # Wait for data from server without timeout
                message = await websocket.recv()
                
                if isinstance(message, bytes) and mpv_process.stdin:
                    mpv_process.stdin.write(message)
                    mpv_process.stdin.flush()
            except websockets.exceptions.ConnectionClosed:
                logger.error("WebSocket connection closed in speak")
                break
            except Exception as e:
                logger.error(f"Error in speak function: {e}")
                break
    finally:
        if mpv_process.stdin:
            mpv_process.stdin.close()
        mpv_process.wait()
        logger.info("Speaker process closed.")
        shutdown_event.set()  # Signal to other tasks to shutdown

async def main():
    while True:
        shutdown_event = asyncio.Event()
        listen_task = None
        speak_task = None
        
        try:
            async with websockets.connect(URI) as websocket:
                logger.info(f"Connected to {URI}")
                await websocket.send("1")  # Send device index to server
                
                # Create tasks
                listen_task = asyncio.create_task(listen(websocket, shutdown_event))
                speak_task = asyncio.create_task(speak(websocket, shutdown_event))
                
                # Wait for any task to complete or for an exception
                done, pending = await asyncio.wait(
                    [listen_task, speak_task], 
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # If we're here, one of the tasks has completed (probably due to an error)
                logger.info("One of the tasks has completed. Shutting down connection.")
                shutdown_event.set()
                
                # Cancel any pending tasks
                for task in pending:
                    task.cancel()
                
                # Wait for all tasks to complete
                await asyncio.gather(*pending, return_exceptions=True)
        except websockets.exceptions.ConnectionClosedError as e:
            logger.error(f"WebSocket connection error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            # Ensure tasks are properly cancelled if they exist and aren't done
            if listen_task and not listen_task.done():
                listen_task.cancel()
            if speak_task and not speak_task.done():
                speak_task.cancel()
            
            # Make sure shutdown_event is set
            shutdown_event.set()
            
            logger.info("Connection closed, retrying in 5 seconds...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())