"""
EXAMPLE 5: Full Event Flow Simulation
=========================================
This brings everything together into a single runnable demo:
  - Pydantic schemas define the event contracts
  - BaseModule provides the plugin architecture
  - Multiple modules publish and consume events
  - The orchestrator subscribes to everything and reasons

Run this WITHOUT Docker/NATS to see the pattern in action.
The modules use simulation mode — the architecture is real,
only the transport is mocked.

  python 05_full_flow.py
"""

import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, Field

# ──────────────────────────────────────────────────────────
# Shared Event Bus (simulates NATS in-memory)
# ──────────────────────────────────────────────────────────
# In production, this is replaced by actual NATS connections.
# The module code stays exactly the same — only the transport
# layer changes. That's the beauty of the abstraction.


class SimulatedBus:
    """In-memory pub/sub bus that mimics NATS behavior."""

    def __init__(self):
        self.subscribers: dict[str, list] = defaultdict(list)
        self.history: list[dict] = []

    def subscribe(self, subject_pattern: str, callback):
        self.subscribers[subject_pattern].append(callback)

    async def publish(self, subject: str, data: dict):
        event_record = {"subject": subject, "data": data, "time": datetime.now(timezone.utc)}
        self.history.append(event_record)

        for pattern, callbacks in self.subscribers.items():
            if self._matches(pattern, subject):
                for cb in callbacks:
                    await cb(subject, data)

    @staticmethod
    def _matches(pattern: str, subject: str) -> bool:
        """Simple wildcard matching: 'events.>' matches 'events.speech.transcription'"""
        if pattern == subject:
            return True
        if pattern.endswith(".>"):
            prefix = pattern[:-2]
            return subject.startswith(prefix)
        if "*" in pattern:
            p_parts = pattern.split(".")
            s_parts = subject.split(".")
            if len(p_parts) != len(s_parts):
                return False
            return all(p == "*" or p == s for p, s in zip(p_parts, s_parts))
        return False


# Global bus instance
bus = SimulatedBus()


# ──────────────────────────────────────────────────────────
# Event Schemas
# ──────────────────────────────────────────────────────────

class BaseEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4())[:8])
    source_module: str
    event_type: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    client_id: str = "client-demo"
    correlation_id: Optional[str] = None

    def subject(self) -> str:
        return f"events.{self.source_module}.{self.event_type}"


class TranscriptionEvent(BaseEvent):
    source_module: str = "speech"
    event_type: str = "transcription"
    text: str
    confidence: float = Field(ge=0.0, le=1.0)


class DefectEvent(BaseEvent):
    source_module: str = "vision"
    event_type: str = "defect"
    defect_type: str
    severity: str
    confidence: float = Field(ge=0.0, le=1.0)
    production_line: str


class AlertEvent(BaseEvent):
    source_module: str = "orchestrator"
    event_type: str = "alert"
    alert_level: str
    message: str
    triggered_by: list[str] = []


# ──────────────────────────────────────────────────────────
# Module Implementations
# ──────────────────────────────────────────────────────────

class SpeechModule:
    """Simulates the speech-to-report module."""

    def __init__(self):
        self.name = "speech"

    async def ingest(self, audio_text: str, confidence: float):
        """Simulate receiving and transcribing audio."""
        event = TranscriptionEvent(
            text=audio_text,
            confidence=confidence,
        )
        print(f"  🎤 [{self.name}] Transcribed: \"{audio_text}\" (conf: {confidence})")
        await bus.publish(event.subject(), event.model_dump())


class VisionModule:
    """Simulates the quality vision module."""

    def __init__(self):
        self.name = "vision"

    async def detect(self, defect_type: str, severity: str, line: str, confidence: float):
        """Simulate detecting a defect in a camera frame."""
        event = DefectEvent(
            defect_type=defect_type,
            severity=severity,
            confidence=confidence,
            production_line=line,
        )
        print(f"  👁️ [{self.name}] Detected: {defect_type} ({severity}) on line {line}")
        await bus.publish(event.subject(), event.model_dump())


class Orchestrator:
    """
    The central brain of IAOP.

    Subscribes to ALL events. Maintains a simple state model.
    When conditions are met (e.g., high-severity defect + verbal report),
    it triggers composite alerts.

    In the real IAOP, this would use Neo4j for state and Ollama for reasoning.
    Here we use simple Python logic to demonstrate the pattern.
    """

    def __init__(self):
        self.name = "orchestrator"
        self.state = {
            "recent_defects": [],
            "recent_transcriptions": [],
            "active_alerts": [],
        }
        # Subscribe to all events
        bus.subscribe("events.>", self.handle_event)

    async def handle_event(self, subject: str, data: dict):
        """Process any incoming event and decide if action is needed."""
        source = data.get("source_module", "?")
        etype = data.get("event_type", "?")

        # IMPORTANT LESSON: Skip events from ourselves!
        # Without this guard, the orchestrator publishes an alert,
        # which matches "events.>", which re-triggers handle_event,
        # which evaluates rules again, which publishes another alert...
        # infinite recursion. This is a REAL bug in event-driven systems.
        # In production NATS, you'd use separate subjects or consumer
        # groups to avoid this. Here we use a simple source check.
        if source == "orchestrator":
            return

        print(f"  🧠 [orchestrator] Received: {subject}")

        # Update state based on event type
        if etype == "transcription":
            self.state["recent_transcriptions"].append(data)
        elif etype == "defect":
            self.state["recent_defects"].append(data)

        # ── REASONING LOGIC ──
        # In production, this is where Ollama + Neo4j come in.
        # The orchestrator asks: "Given the current state of the
        # facility, do I need to take action?"

        await self._evaluate_rules()

    async def _evaluate_rules(self):
        """
        Simple rule engine. In the real IAOP, this would be:
        1. Query Neo4j for current facility state
        2. Apply client-specific rules from YAML config
        3. Use Ollama for contextual reasoning on ambiguous situations
        """
        defects = self.state["recent_defects"]
        transcriptions = self.state["recent_transcriptions"]

        # Rule 1: High severity defect → immediate alert
        for defect in defects:
            if defect.get("severity") in ("high", "critical"):
                if defect.get("event_id") not in [a.get("event_id") for a in self.state["active_alerts"]]:
                    alert = AlertEvent(
                        alert_level="HIGH",
                        message=f"High-severity {defect['defect_type']} on line {defect['production_line']}",
                        triggered_by=[defect["event_id"]],
                    )
                    self.state["active_alerts"].append(defect)
                    print(f"  🚨 [orchestrator] ALERT: {alert.message}")
                    await bus.publish(alert.subject(), alert.model_dump())

        # Rule 2: Verbal report + defect on same line → escalate
        for trans in transcriptions:
            text_lower = trans.get("text", "").lower()
            for defect in defects:
                line = defect.get("production_line", "").lower()
                if line and line.lower() in text_lower:
                    alert = AlertEvent(
                        alert_level="CRITICAL",
                        message=f"Multi-modal confirmation: verbal report + vision defect on {defect['production_line']}",
                        triggered_by=[trans["event_id"], defect["event_id"]],
                    )
                    print(f"  🚨🚨 [orchestrator] CRITICAL ESCALATION: {alert.message}")
                    await bus.publish(alert.subject(), alert.model_dump())
                    # Clear to avoid re-triggering
                    self.state["recent_transcriptions"].remove(trans)
                    return


class DashboardModule:
    """Simulates a dashboard that displays alerts to operators."""

    def __init__(self):
        bus.subscribe("events.orchestrator.alert", self.display_alert)

    async def display_alert(self, subject: str, data: dict):
        level = data.get("alert_level", "?")
        message = data.get("message", "?")
        print(f"  📺 [dashboard] Displaying alert: [{level}] {message}")


# ──────────────────────────────────────────────────────────
# Simulation
# ──────────────────────────────────────────────────────────

async def main():
    logging.basicConfig(level=logging.WARNING)

    print("=" * 64)
    print("IAOP FULL EVENT FLOW SIMULATION")
    print("=" * 64)

    # Initialize all modules
    speech = SpeechModule()
    vision = VisionModule()
    orchestrator = Orchestrator()
    dashboard = DashboardModule()

    # ── Scenario 1: Simple transcription ──
    print("\n─── Scenario 1: Worker makes a routine report ───")
    await speech.ingest(
        "All clear on floor 3, no issues to report",
        confidence=0.96,
    )
    await asyncio.sleep(0.1)

    # ── Scenario 2: Defect detected by vision ──
    print("\n─── Scenario 2: Vision module spots a defect ───")
    await vision.detect(
        defect_type="surface_crack",
        severity="high",
        line="A3",
        confidence=0.91,
    )
    await asyncio.sleep(0.1)

    # ── Scenario 3: Multi-modal confirmation ──
    print("\n─── Scenario 3: Worker verbally reports same issue ───")
    print("    (Orchestrator should correlate this with the vision event)")
    await speech.ingest(
        "I can see a crack forming on production line A3, requesting inspection",
        confidence=0.93,
    )
    await asyncio.sleep(0.1)

    # ── Summary ──
    print("\n" + "=" * 64)
    print(f"Total events on bus: {len(bus.history)}")
    print("Event subjects seen:")
    for record in bus.history:
        print(f"  {record['subject']}")

    print("\n" + "=" * 64)
    print("WHAT JUST HAPPENED:")
    print("  1. Speech module published a transcription → orchestrator noted it")
    print("  2. Vision module published a defect → orchestrator raised an alert")
    print("  3. Speech reported the same issue → orchestrator correlated both")
    print("     sources and ESCALATED to CRITICAL (multi-modal confirmation)")
    print("  4. Dashboard received and displayed the alerts")
    print()
    print("All modules are completely independent. They communicate ONLY")
    print("through the event bus. The speech module has no idea the vision")
    print("module exists. The orchestrator ties everything together.")
    print("=" * 64)


if __name__ == "__main__":
    asyncio.run(main())
