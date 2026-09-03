import asyncio
from datetime import datetime, timezone
import random
import uuid
from typing import AsyncGenerator, Dict, Any

SAMPLE_TOPICS = [
    {"title": "James Webb Space Telescope", "wiki": "enwiki", "topic": "Space Science"},
    {"title": "Quantum Computing", "wiki": "enwiki", "topic": "Computer Science"},
    {"title": "Renewable Energy Transition", "wiki": "enwiki", "topic": "Climate & Energy"},
    {"title": "Artificial General Intelligence", "wiki": "enwiki", "topic": "Artificial Intelligence"},
    {"title": "2026 Winter Olympics", "wiki": "enwiki", "topic": "Sports"},
    {"title": "Superconductivity", "wiki": "enwiki", "topic": "Physics"},
    {"title": "CRISPR Gene Editing", "wiki": "enwiki", "topic": "Biotechnology"},
    {"title": "Mars Sample Return Mission", "wiki": "enwiki", "topic": "Space Exploration"},
    {"title": "Fusion Power Research", "wiki": "enwiki", "topic": "Physics"},
    {"title": "Global Semiconductor Industry", "wiki": "enwiki", "topic": "Technology"},
]

SAMPLE_EDITORS = [
    {"user": "CosmoExplorer", "bot": False},
    {"user": "QuantumDev", "bot": False},
    {"user": "BioCurator", "bot": False},
    {"user": "WikiAutoBot", "bot": True},
    {"user": "AstroGeek", "bot": False},
    {"user": "EditorX99", "bot": False},
    {"user": "CleanUpBot", "bot": True},
]

SAMPLE_COMMENTS = [
    "Updated latest mission telemetry and launch milestone dates.",
    "Expanded section on quantum error correction benchmarks.",
    "Added peer-reviewed citations for experimental findings.",
    "Corrected typographical error in introduction paragraph.",
    "Reverted vandalism and restored verified scientific data.",
    "Added newly announced government funding allocation details.",
    "Updated international consortium partnership agreements.",
]


class SyntheticWikimediaGenerator:
    """
    Generates realistic Wikimedia recent-change events for reproducible testing,
    offline development, burst load simulation, and system verification.
    """

    def __init__(self, events_per_second: int = 10, spike_probability: float = 0.15):
        self.events_per_second = events_per_second
        self.spike_probability = spike_probability
        self._revision_counter = 120000000

    async def event_stream(self) -> AsyncGenerator[Dict[str, Any], None]:
        delay = 1.0 / max(1, self.events_per_second)

        while True:
            # Decide if this tick is part of an activity spike
            is_spike = random.random() < self.spike_probability
            topic_data = SAMPLE_TOPICS[0] if is_spike else random.choice(SAMPLE_TOPICS)
            editor_data = random.choice(SAMPLE_EDITORS)

            self._revision_counter += 1
            parent_rev = self._revision_counter - 1
            byte_diff = random.randint(-100, 1500) if not editor_data["bot"] else random.randint(5, 50)
            change_size = max(500, 45000 + byte_diff)

            raw_event = {
                "id": random.randint(1000000, 9999999),
                "meta": {
                    "id": str(uuid.uuid4()),
                    "uri": f"https://{topic_data['wiki']}.wikipedia.org/wiki/{topic_data['title'].replace(' ', '_')}",
                    "dt": datetime.now(timezone.utc).isoformat(),
                    "domain": f"{topic_data['wiki']}.wikipedia.org",
                },
                "title": topic_data["title"],
                "title_url": f"https://{topic_data['wiki']}.wikipedia.org/wiki/{topic_data['title'].replace(' ', '_')}",
                "comment": random.choice(SAMPLE_COMMENTS),
                "timestamp": int(datetime.now(timezone.utc).timestamp()),
                "user": editor_data["user"],
                "bot": editor_data["bot"],
                "minor": random.random() < 0.25,
                "length": {"old": change_size - byte_diff, "new": change_size},
                "revision": {"old": parent_rev, "new": self._revision_counter},
                "server_name": f"{topic_data['wiki']}.wikipedia.org",
                "wiki": topic_data["wiki"],
                "namespace": 0,
                "type": "edit",
            }

            yield raw_event
            await asyncio.sleep(delay)
