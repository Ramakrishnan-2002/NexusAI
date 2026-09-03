import asyncio
import signal
import sys
from app.core.logging import logger
from workers.stream_ingestor.ingestor import StreamIngestorService


async def main():
    service = StreamIngestorService()

    def handle_signal(sig, frame):
        logger.info("Signal received, stopping Stream Ingestor gracefully...")
        service.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        await service.run()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.critical(f"Stream Ingestor fatal error: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())
