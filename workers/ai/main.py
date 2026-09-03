import asyncio
import signal
from app.core.logging import logger
from workers.ai.ai_worker import AIAnalysisWorker


async def main():
    worker = AIAnalysisWorker()

    def handle_signal(sig, frame):
        logger.info("Signal received, stopping AI Worker gracefully...")
        asyncio.create_task(worker.stop())

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        await worker.run()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.critical(f"AI Worker fatal error: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())
