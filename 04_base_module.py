"""
EXAMPLE 4: The BaseModule Pattern
=====================================
This is the architectural spine of IAOP. Every AI module — speech,
vision, pose, predictive maintenance — inherits from BaseModule.

WHAT THIS GIVES YOU:
- Automatic NATS connection and lifecycle management
- Standardized event publishing with schema validation
- Health checking (so the orchestrator knows modules are alive)
- Graceful shutdown (finish processing before exiting)
- Consistent logging
- A template that makes building new modules trivial

THE PATTERN:
When you write a new module, you only implement two things:
  1. setup()    — configure your AI model, load weights, etc.
  2. process()  — handle one incoming event and optionally publish results.

Everything else — connecting to NATS, subscribing, serializing events,
error handling, lifecycle — is handled by BaseModule.

This is the "plugin architecture" we discussed. A new engineer joining
CoRe can build a new AI module in an afternoon because the boilerplate
is done.

TO RUN:
  python 04_base_module.py
  (simulates the lifecycle without a real NATS connection)
"""

import asyncio
import json
import logging
import signal
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, Field


# ══════════════════════════════════════════════════════════════
# Event Schema (simplified from Example 3)
# ══════════════════════════════════════════════════════════════

class BaseEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    source_module: str
    event_type: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    client_id: Optional[str] = None
    correlation_id: Optional[str] = None

    def to_nats_subject(self) -> str:
        return f"events.{self.source_module}.{self.event_type}"

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> "BaseEvent":
        return cls.model_validate_json(data)


# ══════════════════════════════════════════════════════════════
# PART 1: The BaseModule Abstract Class
# ══════════════════════════════════════════════════════════════

class BaseModule(ABC):
    """
    Abstract base class for all IAOP AI modules.

    Every module in the platform inherits from this class. It handles:
    - NATS connection and JetStream setup
    - Subscribing to input subjects
    - Publishing output events with schema validation
    - Health heartbeats
    - Graceful shutdown

    To create a new module, subclass BaseModule and implement:
      - setup(): one-time initialization (load models, connect to hardware)
      - process(subject, data): handle one incoming message

    That's it. The rest is handled for you.
    """

    def __init__(
        self,
        module_name: str,
        subscribe_subjects: list[str],
        nats_url: str = "nats://localhost:4222",
        client_id: Optional[str] = None,
    ):
        # Identity
        self.module_name = module_name
        self.subscribe_subjects = subscribe_subjects
        self.nats_url = nats_url
        self.client_id = client_id

        # State
        self._running = False
        self._nc = None       # NATS connection
        self._js = None       # JetStream context
        self._subscriptions = []

        # Logging — each module gets its own named logger
        self.logger = logging.getLogger(f"iaop.{module_name}")

    # ──────────────────────────────────────────────────────────
    # Abstract methods — YOU implement these in your module
    # ──────────────────────────────────────────────────────────

    @abstractmethod
    async def setup(self) -> None:
        """
        Called once when the module starts.
        Use this to load your AI model, connect to cameras,
        initialize hardware, etc.

        Example:
            self.model = load_whisper_model("large-v3")
            self.logger.info("Whisper model loaded")
        """
        ...

    @abstractmethod
    async def process(self, subject: str, data: dict) -> Optional[BaseEvent]:
        """
        Called for each incoming message.

        Args:
            subject: The NATS subject (e.g. "events.speech.audio_ready")
            data: The deserialized JSON payload

        Returns:
            Optionally return a BaseEvent to publish as a result.
            Return None if this message doesn't produce output.

        Example:
            text = self.model.transcribe(data["audio_path"])
            return TranscriptionEvent(text=text, confidence=0.95)
        """
        ...

    # ──────────────────────────────────────────────────────────
    # Lifecycle — BaseModule handles all of this
    # ──────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Boot sequence:
        1. Connect to NATS
        2. Set up JetStream
        3. Call your setup() method
        4. Subscribe to input subjects
        5. Start health heartbeat
        6. Enter the event processing loop
        """
        self.logger.info(f"Starting module '{self.module_name}'...")

        # In production, this connects to real NATS.
        # For this demo, we simulate it.
        try:
            import nats
            self._nc = await nats.connect(self.nats_url)
            self._js = self._nc.jetstream()
            self.logger.info(f"Connected to NATS at {self.nats_url}")
        except Exception as e:
            self.logger.warning(f"NATS not available ({e}), running in simulation mode")
            self._nc = None
            self._js = None

        # Call the module's setup (load models, etc.)
        await self.setup()
        self.logger.info("Module setup complete")

        # Subscribe to input subjects
        if self._nc:
            for subject in self.subscribe_subjects:
                sub = await self._nc.subscribe(
                    subject,
                    cb=self._message_handler,
                )
                self._subscriptions.append(sub)
                self.logger.info(f"Subscribed to '{subject}'")

        self._running = True
        self.logger.info(f"Module '{self.module_name}' is READY")

        # Start heartbeat (tells the orchestrator "I'm alive")
        if self._nc:
            asyncio.create_task(self._heartbeat_loop())

    async def _message_handler(self, msg) -> None:
        """
        Internal handler for incoming NATS messages.
        Deserializes, calls your process(), and publishes the result.
        You never call this directly.
        """
        try:
            data = json.loads(msg.data.decode())
            self.logger.debug(f"Processing message on '{msg.subject}'")

            # Call YOUR implementation
            result = await self.process(msg.subject, data)

            # If process() returned an event, publish it
            if result and isinstance(result, BaseEvent):
                await self.publish(result)

            # Acknowledge (if JetStream)
            if hasattr(msg, "ack"):
                await msg.ack()

        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON on '{msg.subject}': {msg.data}")
        except Exception as e:
            self.logger.error(f"Error processing message: {e}", exc_info=True)

    async def publish(self, event: BaseEvent) -> None:
        """
        Publish an event to NATS.
        The event's Pydantic model handles serialization.
        The subject is derived from the event's source_module and event_type.
        """
        subject = event.to_nats_subject()
        payload = event.to_bytes()

        if self._js:
            # Use JetStream for persistent publishing
            ack = await self._js.publish(subject, payload)
            self.logger.debug(f"Published to '{subject}' (seq: {ack.seq})")
        elif self._nc:
            # Fall back to core NATS
            await self._nc.publish(subject, payload)
            self.logger.debug(f"Published to '{subject}' (core)")
        else:
            # Simulation mode
            self.logger.info(f"[SIM] Would publish to '{subject}': {payload.decode()[:80]}...")

    async def _heartbeat_loop(self) -> None:
        """Publish a heartbeat every 10 seconds so the orchestrator
        knows this module is alive and healthy."""
        while self._running:
            if self._nc:
                await self._nc.publish(
                    f"health.{self.module_name}",
                    json.dumps({
                        "module": self.module_name,
                        "status": "healthy",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }).encode(),
                )
            await asyncio.sleep(10)

    async def stop(self) -> None:
        """Graceful shutdown: unsubscribe, drain connection, cleanup."""
        self.logger.info(f"Shutting down module '{self.module_name}'...")
        self._running = False

        for sub in self._subscriptions:
            await sub.unsubscribe()

        if self._nc:
            await self._nc.drain()

        self.logger.info(f"Module '{self.module_name}' stopped cleanly")

    async def run_forever(self) -> None:
        """Convenience method to start and run until interrupted."""
        await self.start()

        # Handle SIGINT/SIGTERM for graceful shutdown
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))
            except NotImplementedError:
                pass  # Windows doesn't support add_signal_handler

        while self._running:
            await asyncio.sleep(1)


# ══════════════════════════════════════════════════════════════
# PART 2: A Real Module Example — Speech to Report
# ══════════════════════════════════════════════════════════════
# This is what building a new IAOP module looks like.
# Notice how SHORT it is. All the infrastructure is inherited.

class TranscriptionEvent(BaseEvent):
    source_module: str = "speech_to_report"
    event_type: str = "transcription"
    text: str
    confidence: float = Field(ge=0.0, le=1.0)
    language: str = "en"


class SpeechToReportModule(BaseModule):
    """
    IAOP module: converts speech audio into structured text reports.

    Subscribes to: events.ingest.audio (raw audio from microphones)
    Publishes:     events.speech_to_report.transcription
    """

    def __init__(self, **kwargs):
        super().__init__(
            module_name="speech_to_report",
            subscribe_subjects=["events.ingest.audio"],
            **kwargs,
        )
        self.model = None  # Will be set in setup()

    async def setup(self) -> None:
        """Load the speech recognition model."""
        # In production:
        #   import faster_whisper
        #   self.model = faster_whisper.WhisperModel("large-v3")
        #
        # For this demo, we simulate it:
        self.logger.info("Loading Whisper model (simulated)...")
        await asyncio.sleep(0.5)  # Simulate model loading time
        self.model = "whisper-large-v3-simulated"
        self.logger.info("Whisper model loaded")

    async def process(self, subject: str, data: dict) -> Optional[BaseEvent]:
        """
        Process one audio event:
        1. Get the audio file path
        2. Run speech recognition
        3. Return a TranscriptionEvent
        """
        audio_path = data.get("audio_path", "unknown")
        self.logger.info(f"Transcribing: {audio_path}")

        # In production:
        #   segments, info = self.model.transcribe(audio_path)
        #   text = " ".join(s.text for s in segments)
        #   confidence = sum(s.avg_logprob for s in segments) / len(segments)
        #
        # Simulated:
        text = "Worker reported unusual vibration in pump station 3"
        confidence = 0.94

        return TranscriptionEvent(
            text=text,
            confidence=confidence,
            client_id=data.get("client_id"),
            correlation_id=data.get("correlation_id"),
        )


# ══════════════════════════════════════════════════════════════
# PART 3: Demo — Simulate the Module Lifecycle
# ══════════════════════════════════════════════════════════════

async def demo():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)-24s | %(levelname)-5s | %(message)s",
        datefmt="%H:%M:%S",
    )

    print("=" * 60)
    print("BASE MODULE PATTERN — DEMO")
    print("=" * 60)

    # Create the module
    module = SpeechToReportModule()

    # Start it (connects to NATS if available, loads model)
    await module.start()

    # Simulate receiving an audio event
    print("\n--- Simulating incoming audio event ---")
    simulated_event = {
        "audio_path": "/data/audio/recording_001.wav",
        "client_id": "client-metfab-01",
        "correlation_id": "session-42",
    }

    # In production, this comes from NATS. Here we call process() directly.
    result = await module.process("events.ingest.audio", simulated_event)

    if result:
        print(f"\n--- Module Output ---")
        print(f"  Subject:     {result.to_nats_subject()}")
        print(f"  Text:        {result.text}")
        print(f"  Confidence:  {result.confidence}")
        print(f"  Client:      {result.client_id}")
        print(f"  Correlation: {result.correlation_id}")
        print(f"\n  JSON payload:")
        print(f"  {result.to_bytes().decode()}")

        # Publish (will simulate since NATS likely isn't running)
        await module.publish(result)

    await module.stop()

    print("\n" + "=" * 60)
    print("KEY TAKEAWAY:")
    print("Building a new module = subclass BaseModule + implement")
    print("setup() and process(). That's it. ~30 lines of actual logic.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(demo())
