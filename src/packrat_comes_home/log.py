# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:

from __future__ import annotations

import logging
import warnings
from multiprocessing import Queue
from pathlib import Path
from threading import Thread

logger = logging.getLogger("packrat")
logging_thread: LoggingThread | None = None


def _showwarning(message, category, filename, lineno, file=None, line=None):
    logger = logging.getLogger("py.warnings")
    logger.warning(
        warnings.formatwarning(message, category, filename, lineno, line),
        stack_info=True,
    )


def setup_logging(level: str | int, log_path: str | Path) -> None:
    root = logging.getLogger()
    root.setLevel(level)

    dropbox = logging.getLogger("dropbox")
    dropbox.setLevel(logging.WARNING)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)8s] %(funcName)s: "
        "%(message)s (%(filename)s:%(lineno)s)"
    )

    handlers: list[logging.Handler] = [
        logging.StreamHandler(),
        logging.FileHandler(log_path, "a", errors="backslashreplace"),
    ]
    for handler in handlers:
        handler.setFormatter(formatter)
        root.addHandler(handler)

    warnings.showwarning = _showwarning

    global logging_thread
    logging_thread = LoggingThread()
    logging_thread.start()


class LoggingThread(Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.logging_queue: Queue[logging.LogRecord] = Queue()

    def run(self) -> None:
        while True:
            record = self.logging_queue.get()
            logger = logging.getLogger(record.name)
            logger.handle(record)
