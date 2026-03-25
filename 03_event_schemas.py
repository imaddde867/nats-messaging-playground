"""
EXAMPLE 3: Pydantic Event Schemas
=====================================
This is arguably the most important file in this learning package,
because it answers the question: "What exactly IS an event, and how
do I make sure every module speaks the same language?"

THE PROBLEM:
Without a schema, Module A publishes:
  {"txt": "valve leak", "conf": 0.9, "ts": "2026-03-25"}
And Module B publishes:
  {"text": "defect found", "confidence": "high", "timestamp": 1711360000}

Now the orchestrator has to handle both formats. Different field names,
different types ("high" vs 0.9), different date formats. This is chaos.
And it gets worse with every new module.

THE SOLUTION: Pydantic models as event contracts.
Pydantic is a Python library for data validation. You define a Python
class that describes the exact shape of your data, and Pydantic
guarantees that:
  - All required fields are present
  - All fields have the correct type
  - Invalid data raises a clear error BEFORE it enters your system
  - Serialization to/from JSON is automatic

Think of it as a contract: "Every event published to events.speech.*
MUST have these fields, with these types." If a module tries to publish
garbage, Pydantic catches it immediately — not three services downstream
when something crashes at 2 AM.

WHY NOT JUST USE DICTS?
You could. But dicts have no validation, no autocomplete in your IDE,
no documentation, and no way to catch mistakes until runtime. Pydantic
models give you all of those for free. When your codebase has 6+ modules
all publishing and consuming events, schemas are the difference between
a maintainable system and a nightmare.

TO RUN:
  python 03_event_schemas.py
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


# ══════════════════════════════════════════════════════════════
# PART 1: The Base Event
# ══════════════════════════════════════════════════════════════
# Every single event in IAOP inherits from this base class.
# It guarantees that EVERY event, regardless of which module
# produced it, has a consistent envelope of metadata.


class BaseEvent(BaseModel):
    """
    The universal event envelope for IAOP.

    Every event in the system — speech transcriptions, vision detections,
    pose violations, orchestrator decisions — wraps its payload in this
    envelope. The orchestrator can process ANY event's metadata without
    knowing the specific event type.

    This is the "event schema" we keep talking about.
    """

    # A unique ID for this specific event instance.
    # Default factory generates a new UUID for each event.
    event_id: str = Field(default_factory=lambda: str(uuid4()))

    # Which module produced this event (e.g. "speech_to_report", "quality_vision")
    source_module: str

    # The event type, matching the NATS subject suffix
    # (e.g. "transcription", "defect_detected", "zone_violation")
    event_type: str

    # ISO 8601 timestamp — when the event was created.
    # Default factory captures the current time.
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    # Optional: the client/facility this event belongs to.
    # This is how you support multi-client deployments.
    client_id: Optional[str] = None

    # Optional: correlation ID for tracking a chain of related events.
    # E.g., a speech transcription triggers a report generation,
    # which triggers an alert — all share the same correlation_id.
    correlation_id: Optional[str] = None

    def to_nats_subject(self) -> str:
        """Generate the NATS subject for this event.

        Convention: events.{source_module}.{event_type}
        Example: events.speech_to_report.transcription
        """
        return f"events.{self.source_module}.{self.event_type}"

    def to_bytes(self) -> bytes:
        """Serialize to bytes for NATS publishing."""
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> "BaseEvent":
        """Deserialize from NATS message bytes."""
        return cls.model_validate_json(data)


# ══════════════════════════════════════════════════════════════
# PART 2: Module-Specific Events
# ══════════════════════════════════════════════════════════════
# Each module defines its own event types that extend BaseEvent.
# The base envelope stays the same; the payload differs.


# --- Speech Module Events ---

class TranscriptionEvent(BaseEvent):
    """Published when the speech module completes a transcription."""

    source_module: str = "speech_to_report"
    event_type: str = "transcription"

    # The transcribed text
    text: str

    # Confidence score from the speech recognition model (0.0 to 1.0)
    confidence: float = Field(ge=0.0, le=1.0)

    # Duration of the audio clip in seconds
    audio_duration_seconds: Optional[float] = None

    # Language detected
    language: str = "en"

    @field_validator("confidence")
    @classmethod
    def round_confidence(cls, v: float) -> float:
        """Round confidence to 3 decimal places for consistency."""
        return round(v, 3)


class ReportGeneratedEvent(BaseEvent):
    """Published when a transcription has been converted into a report."""

    source_module: str = "speech_to_report"
    event_type: str = "report_generated"

    # Reference to the original transcription
    transcription_event_id: str

    # Path to the generated report file
    report_path: str

    # Report format
    report_format: str = "pdf"


# --- Vision Module Events ---

class SeverityLevel(str, Enum):
    """Enum ensures severity is always one of these values — never 'high-ish'."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class DefectDetectedEvent(BaseEvent):
    """Published when the vision module detects a manufacturing defect."""

    source_module: str = "quality_vision"
    event_type: str = "defect_detected"

    defect_type: str          # e.g. "scratch", "dent", "misalignment"
    severity: SeverityLevel   # Enum — only valid values accepted
    confidence: float = Field(ge=0.0, le=1.0)
    production_line: str      # e.g. "A3", "B1"

    # Optional bounding box of the defect in the image [x, y, width, height]
    bounding_box: Optional[list[float]] = None

    # Optional path to the captured frame
    frame_path: Optional[str] = None


# --- Pose Module Events ---

class ZoneViolationEvent(BaseEvent):
    """Published when a worker enters a restricted zone."""

    source_module: str = "pose_detection"
    event_type: str = "zone_violation"

    worker_id: str
    zone_name: str
    violation_type: str = "unauthorized_entry"
    confidence: float = Field(ge=0.0, le=1.0)


# ══════════════════════════════════════════════════════════════
# PART 3: Let's See It Work
# ══════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("PYDANTIC EVENT SCHEMAS — DEMO")
    print("=" * 60)

    # --- Creating events ---
    print("\n1. Creating a valid TranscriptionEvent:")
    event = TranscriptionEvent(
        text="Worker reported unusual noise from compressor unit 9",
        confidence=0.5,
        audio_duration_seconds=4.2,
        client_id="client-metfab-01",
    )
    event2 = DefectDetectedEvent(
        SeverityLevel='low'
    )
    event3 = DefectDetectedEvent(
        SeverityLevel='very_high'
    )
    print('1:')
    print(f"   Event ID:  {event.event_id}")
    print(f"   Subject:   {event.to_nats_subject()}")
    print(f"   Timestamp: {event.timestamp}")
    print(f"   Text:      {event.text}")
    print(f"   Conf:      {event.confidence}")
    print('2')
    print(f"   Severity:  {event2.SeverityLevel}")
    print('3')
    print(f"   Severity:  {event3.SeverityLevel}")
    
    # --- Serialization (what goes on the wire to NATS) ---
    print("\n2. Serialized to JSON (this is what NATS transmits):")
    json_bytes = event.to_bytes()
    json_bytes2 = event2.to_bytes()
    json_bytes3 = event3.to_bytes()
    print(f"   1- {json_bytes.decode()[:120]}...")
    print(f"   2- {json_bytes2.decode()[:120]}...")
    print(f"   3- {json_bytes3.decode()[:120]}...")

    # --- Deserialization (what the consumer receives) ---
    print("\n3. Deserializing back from bytes:")
    restored = TranscriptionEvent.from_bytes(json_bytes)
    restored2 = TranscriptionEvent.from_bytes(json_bytes2)
    restored3 = TranscriptionEvent.from_bytes(json_bytes3)
    print(f"1- Text: {restored.text}")
    print(f"   Same event? {restored.event_id == event.event_id}")
    print(f"2- Text: {restored.text}")
    print(f"   Same event? {restored2.event_id == event2.event_id}")
    print(f"3- Text: {restored.text}")
    print(f"   Same event? {restored3.event_id == event3.event_id}")


    # --- Validation catches bad data ---
    print("\n4. Validation in action — trying invalid confidence (1.5):")
    try:
        bad_event = TranscriptionEvent(
            text="This should fail",
            confidence=1.5,  # Invalid! Must be 0.0-1.0
        )
    except Exception as e:
        print(f"   ✓ Caught: {e}")

    print("\n5. Validation — trying invalid severity:")
    try:
        bad_defect = DefectDetectedEvent(
            defect_type="scratch",
            severity="kinda bad",  # Invalid! Must be low/medium/high/critical
            confidence=0.8,
            production_line="A3",
        )
    except Exception as e:
        error_msg = str(e)
        print(f"   ✓ Caught: {error_msg[:100]}...")

    # --- Multi-event scenario ---
    print("\n6. Simulating a real event chain:")
    # Speech module transcribes audio
    transcription = TranscriptionEvent(
        text="Crack detected on beam section 4B, requesting inspection",
        confidence=0.92,
        client_id="client-construction-03",
        correlation_id="chain-001",
    )
    print(f"   → {transcription.to_nats_subject()}: \"{transcription.text}\"")

    # Vision module independently detects the same issue
    defect = DefectDetectedEvent(
        defect_type="crack",
        severity=SeverityLevel.HIGH,
        confidence=0.88,
        production_line="4B",
        client_id="client-construction-03",
        correlation_id="chain-001",  # Same correlation — linked!
    )
    print(f"   → {defect.to_nats_subject()}: {defect.defect_type} ({defect.severity.value})")

    # The orchestrator sees BOTH events, notices they share a correlation_id,
    # and realizes: "Two independent modules detected the same problem.
    # This is a high-confidence, multi-modal alert."
    print(f"\n   🧠 Orchestrator: Two correlated events detected!")
    print(f"      Correlation: {transcription.correlation_id}")
    print(f"      Speech confidence: {transcription.confidence}")
    print(f"      Vision confidence: {defect.confidence}")
    print(f"      Combined action: ESCALATE to supervisor")

    # --- Schema as documentation ---
    print("\n7. Schema as documentation (JSON Schema export):")
    schema = TranscriptionEvent.model_json_schema()
    print(f"   Required fields: {schema.get('required', [])}")
    print(f"   Properties: {list(schema.get('properties', {}).keys())}")
    # This JSON schema can be shared with other teams, used in docs,
    # or even used to generate client libraries in other languages.


if __name__ == "__main__":
    main()
