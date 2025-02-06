from queue import Queue


class LogHandler:
    # ИНИЦИАЛИЗАЦИЯ;
    def __init__(self, log_queue: Queue) -> None:
        self._log_queue: Queue = log_queue

    # МЕТОДЫ;
    def write(self, message: str) -> None:
        self._log_queue.put(message)

    def flush(self) -> None:
        pass
