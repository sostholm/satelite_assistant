import asyncio
import logging
import os
from typing import Dict

import pyaudio
import requests
from livekit import rtc

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

LIVEKIT_URL = os.environ.get("LIVEKIT_URL", "ws://localhost:7880")
TOKEN_ENDPOINT = os.environ["TOKEN_ENDPOINT"]  # e.g. http://backend/livekit-token

DEVICE_ID = os.environ.get("DEVICE_ID", "1")
INPUT_DEVICE_INDEX = int(os.environ.get("INPUT_DEVICE_INDEX", 2))
OUTPUT_DEVICE_INDEX = os.environ.get("OUTPUT_DEVICE_INDEX")  # optional

# Per-satellite room + identity
ROOM_NAME = f"satellite-{DEVICE_ID}"
IDENTITY = f"sat-{DEVICE_ID}"

# Audio params
FORMAT = pyaudio.paInt16
CHANNELS = 1
SAMPLE_RATE = 16000  # matches your existing MIC_RATE
SPEAKER_SAMPLE_RATE = 24000  # Higher quality for TTS
CHUNK_MS = 20
SAMPLES_PER_CHUNK = SAMPLE_RATE * CHUNK_MS // 1000

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("satellite-livekit")


# -----------------------------------------------------------------------------
# Token fetch
# -----------------------------------------------------------------------------


def fetch_token(room_name: str, identity: str) -> str:
    """
    Ask your backend for a LiveKit access token for this satellite.

    The backend should generate a JWT with VideoGrants(room_join=True, room=room_name)
    and return JSON like: { "token": "<jwt>" }.
    """
    logger.info("Requesting token for room=%s identity=%s", room_name, identity)
    resp = requests.get(
        TOKEN_ENDPOINT,
        params={"room": room_name, "identity": identity},
        timeout=5,
    )
    resp.raise_for_status()
    data = resp.json()
    token = data["token"]
    return token


# -----------------------------------------------------------------------------
# Mic → LiveKit
# -----------------------------------------------------------------------------


async def publish_mic(room: rtc.Room, shutdown_event: asyncio.Event) -> None:
    """
    Capture audio from the local microphone and publish it as a LiveKit audio track.
    """
    loop = asyncio.get_running_loop()
    mic_queue: asyncio.Queue[bytes] = asyncio.Queue()

    pa = pyaudio.PyAudio()

    # Log devices for debugging (once at start)
    try:
        logger.info("Listing audio input devices:")
        for i in range(pa.get_device_count()):
            dev_info = pa.get_device_info_by_index(i)
            logger.info(
                "  Input device %d: %s (max input channels=%s)",
                i,
                dev_info.get("name"),
                dev_info.get("maxInputChannels"),
            )
    except Exception as e:
        logger.warning("Failed to list audio devices: %s", e)

    def callback(in_data, frame_count, time_info, status):
        # Called by PyAudio's internal thread
        asyncio.run_coroutine_threadsafe(mic_queue.put(in_data), loop)
        return (None, pyaudio.paContinue)

    stream_in = pa.open(
        format=FORMAT,
        channels=CHANNELS,
        rate=SAMPLE_RATE,
        input=True,
        frames_per_buffer=SAMPLES_PER_CHUNK,
        input_device_index=INPUT_DEVICE_INDEX,
        stream_callback=callback,
    )
    stream_in.start_stream()
    logger.info("Microphone started on device index %d", INPUT_DEVICE_INDEX)

    # LiveKit audio source + local track
    source = rtc.AudioSource(
        sample_rate=SAMPLE_RATE,
        num_channels=CHANNELS,
        loop=loop,
    )
    track = rtc.LocalAudioTrack.create_audio_track("mic", source)

    options = rtc.TrackPublishOptions()
    options.source = rtc.TrackSource.SOURCE_MICROPHONE

    await room.local_participant.publish_track(track, options)
    logger.info("Published mic track to room")

    try:
        while not shutdown_event.is_set() and room.isconnected():
            data = await mic_queue.get()
            # Bytes -> AudioFrame (16-bit signed PCM)
            samples_per_channel = len(data) // (2 * CHANNELS)

            try:
                frame = rtc.AudioFrame(
                    data=data,
                    sample_rate=SAMPLE_RATE,
                    num_channels=CHANNELS,
                    samples_per_channel=samples_per_channel,
                )
            except ValueError as e:
                logger.error("Invalid audio frame size: %s", e)
                continue

            try:
                await source.capture_frame(frame)
            except Exception as e:
                logger.error("Error capturing frame into LiveKit: %s", e)
                shutdown_event.set()
                break

    except Exception as e:
        logger.error("Unexpected error in publish_mic: %s", e)
        shutdown_event.set()
    finally:
        logger.info("Stopping microphone")
        try:
            stream_in.stop_stream()
            stream_in.close()
        except Exception:
            pass
        pa.terminate()
        await source.aclose()
        logger.info("Microphone & AudioSource cleaned up")


# -----------------------------------------------------------------------------
# LiveKit → Speaker
# -----------------------------------------------------------------------------


async def play_remote_audio(
    track: rtc.RemoteAudioTrack,
    shutdown_event: asyncio.Event,
) -> None:
    """
    Subscribe to a remote audio track and play it to the local speaker.
    """
    loop = asyncio.get_running_loop()

    pa = pyaudio.PyAudio()

    output_device_index = None
    if OUTPUT_DEVICE_INDEX is not None:
        try:
            output_device_index = int(OUTPUT_DEVICE_INDEX)
        except ValueError:
            logger.warning(
                "Invalid OUTPUT_DEVICE_INDEX=%r, falling back to default output",
                OUTPUT_DEVICE_INDEX,
            )

    stream_out = pa.open(
        format=FORMAT,
        channels=CHANNELS,
        rate=SPEAKER_SAMPLE_RATE,
        output=True,
        frames_per_buffer=SAMPLES_PER_CHUNK,
        output_device_index=output_device_index,
    )
    logger.info(
        "Speaker started on device index %s",
        output_device_index if output_device_index is not None else "(default)",
    )

    # Turn remote track into an async iterator of AudioFrames
    audio_stream = rtc.AudioStream.from_track(
        track=track,
        sample_rate=SPEAKER_SAMPLE_RATE,
        num_channels=CHANNELS,
        frame_size_ms=CHUNK_MS,
    )

    try:
        async for event in audio_stream:
            if shutdown_event.is_set():
                break

            frame = event.frame
            # frame.data is memoryview(int16); tobytes() gives raw bytes for PyAudio
            pcm_bytes = frame.data.tobytes()

            try:
                # Run blocking write in executor so we don't block the event loop
                await loop.run_in_executor(None, stream_out.write, pcm_bytes)
            except Exception as e:
                logger.error("Error writing to speaker: %s", e)
                shutdown_event.set()
                break

    except Exception as e:
        logger.error("Unexpected error in play_remote_audio: %s", e)
        shutdown_event.set()
    finally:
        logger.info("Stopping remote audio playback for track %s", track.sid)
        try:
            stream_out.stop_stream()
            stream_out.close()
        except Exception:
            pass
        pa.terminate()
        try:
            await audio_stream.aclose()
        except Exception:
            pass


# -----------------------------------------------------------------------------
# Main room loop with reconnect
# -----------------------------------------------------------------------------


async def run_client() -> None:
    """
    Connects to LiveKit, publishes mic, plays remote audio, and reconnects on failure.
    """
    while True:
        room = rtc.Room()
        shutdown_event = asyncio.Event()
        remote_audio_tasks: Dict[str, asyncio.Task] = {}
        mic_task: asyncio.Task | None = None

        @room.on("connected")
        def _on_connected():
            logger.info(
                "Connected to LiveKit room=%s as identity=%s", ROOM_NAME, IDENTITY
            )

        @room.on("disconnected")
        def _on_disconnected(reason):
            logger.info("Disconnected from room (%s)", reason)
            shutdown_event.set()

        @room.on("track_subscribed")
        def _on_track_subscribed(track, publication, participant):
            if isinstance(track, rtc.RemoteAudioTrack):
                logger.info(
                    "Subscribed to remote audio from participant=%s track_sid=%s",
                    participant.identity,
                    track.sid,
                )
                task = asyncio.create_task(
                    play_remote_audio(track, shutdown_event),
                    name=f"play_remote_audio_{track.sid}",
                )
                remote_audio_tasks[track.sid] = task

        @room.on("track_unsubscribed")
        def _on_track_unsubscribed(track, publication, participant):
            if isinstance(track, rtc.RemoteAudioTrack):
                logger.info(
                    "Unsubscribed from remote audio track_sid=%s participant=%s",
                    track.sid,
                    participant.identity,
                )
                task = remote_audio_tasks.pop(track.sid, None)
                if task:
                    task.cancel()

        try:
            token = fetch_token(ROOM_NAME, IDENTITY)

            # Connect (this implicitly creates the room on first join)
            await room.connect(LIVEKIT_URL, token)
            logger.info("room.connect completed")

            # Start mic publishing
            mic_task = asyncio.create_task(
                publish_mic(room, shutdown_event),
                name="publish_mic",
            )

            # Wait until something triggers shutdown_event
            await shutdown_event.wait()

        except Exception as e:
            logger.error("Error in main room loop: %s", e)

        finally:
            logger.info("Cleaning up room session")

            if mic_task is not None and not mic_task.done():
                mic_task.cancel()
                try:
                    await mic_task
                except asyncio.CancelledError:
                    pass

            for t in list(remote_audio_tasks.values()):
                t.cancel()
            if remote_audio_tasks:
                await asyncio.gather(
                    *remote_audio_tasks.values(), return_exceptions=True
                )

            try:
                if room.isconnected():
                    await room.disconnect()
            except Exception:
                pass

            logger.info("Reconnect in 5 seconds...")
            await asyncio.sleep(5)


def main() -> None:
    try:
        asyncio.run(run_client())
    except KeyboardInterrupt:
        logger.info("Satellite client stopped by user")


if __name__ == "__main__":
    main()
