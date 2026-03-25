"""
EXAMPLE 2: NATS JetStream — Persistent Messaging
====================================================
Core NATS is fire-and-forget: if nobody is listening, the message is gone.
JetStream adds PERSISTENCE. Messages are stored to disk and can be replayed.

WHY JETSTREAM MATTERS FOR IAOP:
Imagine your orchestrator crashes and restarts. With core NATS, it missed
every event that happened while it was down. With JetStream, it picks up
exactly where it left off — no data loss.

JetStream also adds:
- Acknowledgments: consumers confirm they processed a message
- Replay: new consumers can read historical messages
- Retention: messages stay until explicitly deleted or aged out
- Exactly-once delivery: prevents duplicate processing

THE KEY CONCEPTS:

STREAM = A named, persistent log of messages.
  Think of it as a database table that stores every event.
  You define which subjects it captures.
  Example: a stream called "EVENTS" captures "events.>" (all events).

CONSUMER = A named subscription with tracked position.
  Think of it as a cursor in a database. It remembers where you are.
  If your service restarts, the consumer resumes from the last
  acknowledged message — not from the beginning.

TO RUN:
1. Start NATS: docker compose up -d
2. Run this file: python 02_jetstream.py
"""

import asyncio
import json
from datetime import datetime

import nats
from nats.js.api import StreamConfig, ConsumerConfig, DeliverPolicy, AckPolicy


async def main():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()  # Get the JetStream context
    print("✓ Connected to NATS JetStream")

    # ──────────────────────────────────────────────────────────
    # STEP 1: Create a Stream
    # ──────────────────────────────────────────────────────────
    # A stream captures messages from specific subjects and stores
    # them persistently. This is where your event history lives.
    #
    # For IAOP, you'd have one stream that captures ALL events:
    #   subjects=["events.>"]
    # This means every message published to events.speech.*,
    # events.vision.*, events.pose.*, etc. is stored.

    try:
        # add_stream creates it if it doesn't exist, or returns existing
        await js.add_stream(
            StreamConfig(
                name="EVENTS",              # Stream name (uppercase by convention)
                subjects=["events.>"],      # Capture all events
                retention="limits",         # Keep messages until limits are hit
                max_msgs=10000,             # Keep last 10,000 messages
                max_age=86400_000_000_000,  # Keep for 24 hours (nanoseconds)
                storage="file",             # Persist to disk (survives restart)
                num_replicas=1,             # 1 replica (increase for HA clusters)
            )
        )
        print("✓ Stream 'EVENTS' created (captures events.>)")
    except Exception as e:
        print(f"  Stream already exists or error: {e}")

    # ──────────────────────────────────────────────────────────
    # STEP 2: Publish events (they're now persisted!)
    # ──────────────────────────────────────────────────────────
    # With JetStream, publish returns an acknowledgment (PubAck)
    # confirming the message was stored. Unlike core NATS where
    # publish is fire-and-forget, JetStream guarantees storage.

    events = [
        ("events.speech.transcription", {
            "module": "speech_to_report",
            "text": "Valve pressure reading at 4.2 bar",
            "confidence": 0.95,
            "timestamp": datetime.now().isoformat(),
        }),
        ("events.vision.defect", {
            "module": "quality_vision",
            "type": "surface_scratch",
            "severity": "medium",
            "production_line": "A3",
            "timestamp": datetime.now().isoformat(),
        }),
        ("events.pose.violation", {
            "module": "pose_detection",
            "worker_id": "W042",
            "violation": "entered_restricted_zone",
            "zone": "heavy_machinery",
            "timestamp": datetime.now().isoformat(),
        }),
    ]

    print("\nPublishing events to JetStream:")
    for subject, data in events:
        # js.publish returns a PubAck with the stream sequence number
        ack = await js.publish(subject, json.dumps(data).encode())
        print(f"  ✓ Published to '{subject}' → stream seq: {ack.seq}")

    # ──────────────────────────────────────────────────────────
    # STEP 3: Create a Durable Consumer
    # ──────────────────────────────────────────────────────────
    # A durable consumer remembers its position. If your orchestrator
    # restarts, it resumes from where it left off.
    #
    # "deliver_policy=all" means start from the first message.
    # In production, you'd use "deliver_policy=new" for a fresh
    # consumer that only wants future messages, or "last_per_subject"
    # to get the latest state per subject.

    print("\n--- Durable Consumer (orchestrator) ---")

    # Pull-based consumer: you explicitly ask for messages.
    # This gives you backpressure control — your service processes
    # messages at its own pace, not at the publisher's pace.
    consumer = await js.pull_subscribe(
        "events.>",             # Subscribe to all events
        durable="orchestrator", # Durable name — survives restarts
        stream="EVENTS",
    )

    # Fetch messages in batches. In a real service, this runs in a loop.
    messages = await consumer.fetch(batch=10, timeout=2)
    print(f"  Fetched {len(messages)} messages:")

    for msg in messages:
        data = json.loads(msg.data.decode())
        print(f"    [{msg.subject}] module={data.get('module', '?')} → {data}")

        # CRITICAL: Acknowledge the message!
        # This tells JetStream "I processed this, don't redeliver it."
        # If your service crashes before acking, JetStream will
        # redeliver the message to another instance. That's how you
        # get reliability.
        await msg.ack()

    # ──────────────────────────────────────────────────────────
    # STEP 4: Demonstrate replay
    # ──────────────────────────────────────────────────────────
    # A NEW consumer with deliver_policy=all can read ALL historical
    # messages. This is incredibly useful for:
    # - A new dashboard that needs to show today's history
    # - A new analytics module that needs to backfill data
    # - Debugging: "what happened at 14:35?"

    print("\n--- New Consumer (replay from start) ---")
    replay_consumer = await js.pull_subscribe(
        "events.>",
        durable="replay-demo",
        stream="EVENTS",
    )

    replay_msgs = await replay_consumer.fetch(batch=10, timeout=2)
    print(f"  Replayed {len(replay_msgs)} historical messages:")
    for msg in replay_msgs:
        print(f"    [{msg.subject}] {msg.data.decode()[:60]}...")
        await msg.ack()

    # ──────────────────────────────────────────────────────────
    # STEP 5: Stream info (monitoring)
    # ──────────────────────────────────────────────────────────
    # You can query stream state — useful for monitoring dashboards.
    info = await js.stream_info("EVENTS")
    print(f"\n--- Stream Info ---")
    print(f"  Messages stored: {info.state.messages}")
    print(f"  Bytes used: {info.state.bytes}")
    print(f"  Consumers: {info.state.consumer_count}")

    await nc.drain()
    print("\n✓ Done")


if __name__ == "__main__":
    asyncio.run(main())
