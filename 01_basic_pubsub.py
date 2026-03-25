"""
EXAMPLE 1: Basic NATS Pub/Sub
================================
This is the simplest possible interaction with NATS.
One script publishes a message, another subscribes and receives it.

HOW PUB/SUB WORKS:
- A "subject" is like a channel name. Think of it like a radio frequency.
- A publisher sends a message to a subject.
- Any subscriber listening to that subject receives the message.
- The publisher doesn't know (or care) who's listening.
- Multiple subscribers can listen to the same subject.

WHY THIS MATTERS FOR IAOP:
Your speech module publishes "transcription.completed" events.
Your orchestrator subscribes to "transcription.completed".
Your dashboard subscribes to "transcription.completed".
Neither the orchestrator nor the dashboard knows the other exists.
The speech module doesn't know either of them exist.
That's decoupling. That's the whole point.

TO RUN:
1. Start NATS: docker compose up -d
2. Run this file: python 01_basic_pubsub.py
"""

import asyncio
import nats


async def main():
    # ──────────────────────────────────────────────────────────
    # STEP 1: Connect to NATS
    # ──────────────────────────────────────────────────────────
    # This connects to the NATS server running in Docker.
    # "nats://localhost:4222" is the default address.
    # Think of this as opening a socket to the post office.
    nc = await nats.connect("nats://localhost:4222")
    print("✓ Connected to NATS")

    # ──────────────────────────────────────────────────────────
    # STEP 2: Subscribe to a subject
    # ──────────────────────────────────────────────────────────
    # We subscribe BEFORE publishing so we're already listening
    # when the message arrives. In real IAOP, subscribers are
    # always-running services (Docker containers).
    #
    # The subject "events.speech.transcription" uses dots as
    # hierarchy separators. NATS supports wildcard subscriptions:
    #   "events.speech.*"   → matches any speech event
    #   "events.>"          → matches ALL events (any depth)
    # This is powerful: the orchestrator can subscribe to "events.>"
    # and receive every event from every module.

    received_messages = []

    async def message_handler(msg):
        """This function runs every time a message arrives."""
        subject = msg.subject
        data = msg.data.decode()  # Messages are bytes, decode to string
        print(f"  📨 Received on '{subject}': {data}")
        received_messages.append(data)

    # Subscribe to the subject. The callback fires for each message.
    subscription = await nc.subscribe(
        "events.speech.transcription",
        cb=message_handler,
    )
    print("✓ Subscribed to 'events.speech.transcription'")

    # ──────────────────────────────────────────────────────────
    # STEP 3: Publish messages
    # ──────────────────────────────────────────────────────────
    # Publishing is fire-and-forget. The message goes to the
    # broker, and the broker delivers it to all subscribers.
    # The publisher gets no confirmation that anyone received it.
    # (JetStream adds acknowledgments — we'll see that in Example 2.)

    await nc.publish(
        "events.speech.transcription",
        b'{"text": "Worker reported valve leak in section B", "confidence": 0.94}',
    )
    print("✓ Published message 1")

    await nc.publish(
        "events.speech.transcription",
        b'{"text": "All clear on floor 3", "confidence": 0.98}',
    )
    print("✓ Published message 2")

    # Give the async handlers a moment to process
    await asyncio.sleep(0.5)

    # ──────────────────────────────────────────────────────────
    # STEP 4: Wildcard subscriptions
    # ──────────────────────────────────────────────────────────
    # This is where NATS gets really powerful for IAOP.
    # The orchestrator doesn't subscribe to each module's events
    # individually — it subscribes to "events.>" and gets EVERYTHING.

    async def orchestrator_handler(msg):
        print(f"  🧠 Orchestrator sees '{msg.subject}': {msg.data.decode()}")

    await nc.subscribe("events.>", cb=orchestrator_handler)
    print("\n✓ Orchestrator subscribed to 'events.>' (all events)")

    # Now publish different event types — the orchestrator sees them all
    await nc.publish(
        "events.vision.defect",
        b'{"type": "scratch", "severity": "high", "line": "A3"}',
    )
    await nc.publish(
        "events.pose.violation",
        b'{"worker_id": "W042", "zone": "restricted", "confidence": 0.87}',
    )
    await nc.publish(
        "events.speech.transcription",
        b'{"text": "Shutting down line for maintenance", "confidence": 0.91}',
    )

    await asyncio.sleep(0.5)

    # ──────────────────────────────────────────────────────────
    # STEP 5: Request/Reply pattern
    # ──────────────────────────────────────────────────────────
    # NATS also supports request/reply — like HTTP but through
    # the broker. One service asks a question, another answers.
    # Useful for: "Hey orchestrator, what's the current state
    # of production line A3?"

    async def responder(msg):
        """A service that answers questions."""
        print(f"  📋 Got request: {msg.data.decode()}")
        await msg.respond(b'{"line": "A3", "status": "running", "defects_today": 3}')

    await nc.subscribe("query.line.status", cb=responder)

    # Another service asks the question
    response = await nc.request("query.line.status", b'{"line": "A3"}', timeout=2)
    print(f"  ✅ Got reply: {response.data.decode()}")

    # ──────────────────────────────────────────────────────────
    # Cleanup
    # ──────────────────────────────────────────────────────────
    await subscription.unsubscribe()
    await nc.drain()
    print("\n✓ Disconnected cleanly")


if __name__ == "__main__":
    asyncio.run(main())
