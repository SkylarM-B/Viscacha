import threading
import time
from typing import Callable


class Worker:
    """
    Pull jobs off the queue and process them.
    """

    def __init__(self, client: "Client"):
        self._client = client
        self._handlers: dict[str, dict] = {}
        self._running = False

    def job(
        self,
        job_type: str,
        max_retries: int = 3,
        lease_ttl: float = 30.0,
    ) -> Callable:
        """Decorator Register a function as the handler for job_type."""
        def decorator(fn: Callable) -> Callable:
            self._handlers[job_type] = {
                "fn":          fn,
                "max_retries": max_retries,
                "lease_ttl":   lease_ttl,
            }
            return fn
        return decorator

    def run(self, poll_interval: float = 0.5, blocking: bool = True):
        """
        Start the worker loop.
        blocking=True  runs in the current thread (use for scripts / CLI).
        blocking=False  starts a daemon thread, returns it.
        """
        self._running = True
        if blocking:
            self._loop(poll_interval)
        else:
            t = threading.Thread(
                target=self._loop, args=(poll_interval,), daemon=True, name="worker"
            )
            t.start()
            return t

    def stop(self) -> None:
        self._running = False

    #  internal

    def _loop(self, poll_interval: float) -> None:
        while self._running:
            did_work = False
            for job_type, handler in list(self._handlers.items()):
                claimed = self._client._claim(job_type, lease_ttl=handler["lease_ttl"])
                if claimed is None:
                    continue
                did_work = True
                job, lease_id = claimed
                self._process(job, lease_id, handler)
            if not did_work:
                time.sleep(poll_interval)

    def _process(self, job: dict, lease_id: str, handler: dict) -> None:
        try:
            result = handler["fn"](**job["args"])
            self._client._complete(job, lease_id, result)
        except Exception as exc:
            self._client._fail(job, lease_id, str(exc), handler["max_retries"])
